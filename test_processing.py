import unittest
import sqlite3
import os
import json
import datetime
import uuid
import xml.etree.ElementTree as ET
import tkinter as tk
from tkinter import ttk
from unittest.mock import patch, mock_open, MagicMock

# Assuming db_handler.py and OI Import Generator.py are in the same directory or accessible via PYTHONPATH
import db_handler
from OI_Import_Generator import process_row, generate_default_mapping, normalize_mapping, DEFAULT_SPECIAL_CHAR_MAP
# Allow direct manipulation for testing
import OI_Import_Generator as oi_generator

# Helper function to compare XML elements (basic check)
def compare_xml_elements(elem1, elem2):
    if elem1.tag != elem2.tag:
        return False
    if elem1.text is not None and elem2.text is not None:
        if elem1.text.strip() != elem2.text.strip():
            return False
    elif elem1.text is not None or elem2.text is not None: # one has text, other doesn't
        if (elem1.text and elem1.text.strip()) or (elem2.text and elem2.text.strip()): # if the text is not just whitespace
            return False

    if elem1.attrib != elem2.attrib:
        return False
    if len(elem1) != len(elem2):
        return False
    return all(compare_xml_elements(c1, c2) for c1, c2 in zip(elem1, elem2))

class TestDbHandler(unittest.TestCase):
    def setUp(self):
        """Set up a new in-memory database for each test."""
        self.db_path = ":memory:"
        # self.test_db_file = f"test_oi_status_{uuid.uuid4().hex}.db" # Option for temp file
        # self.db_path = self.test_db_file
        self.assertTrue(db_handler.init_db(self.db_path), "Database initialization failed")

    def tearDown(self):
        """Clean up by closing connection; remove file if using temp file."""
        # If using a test file, uncomment:
        # if hasattr(self, 'test_db_file') and os.path.exists(self.test_db_file):
        #     # Explicitly close any connections if module doesn't handle it internally before delete
        #     # For :memory:, connection is closed when it goes out of scope or explicitly
        #     os.remove(self.test_db_file)
        pass

    def test_init_db(self):
        """Test if the 'objects' table and indexes are created."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            # Check for 'objects' table
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='objects';")
            self.assertIsNotNone(cursor.fetchone(), "Table 'objects' was not created.")
            # Check for indexes
            cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_status';")
            self.assertIsNotNone(cursor.fetchone(), "Index 'idx_status' was not created.")
            cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_identifier';")
            self.assertIsNotNone(cursor.fetchone(), "Index 'idx_identifier' was not created.")

    def test_add_pending_objects(self):
        """Test adding new objects and handling existing ones."""
        obj1_id = uuid.uuid4().hex
        obj2_id = uuid.uuid4().hex
        obj3_id = uuid.uuid4().hex

        objects_to_add = [
            {'unique_id': obj1_id, 'csv_row_index': 1, 'csv_data': {'colA': 'val1'}},
            {'unique_id': obj2_id, 'csv_row_index': 2, 'csv_data': {'colA': 'val2'}},
        ]
        added, skipped = db_handler.add_pending_objects(objects_to_add, self.db_path)
        self.assertEqual(added, 2)
        self.assertEqual(skipped, 0)

        # Verify data
        obj1_status = db_handler.get_object_status(obj1_id, self.db_path)
        self.assertIsNotNone(obj1_status)
        self.assertEqual(obj1_status['status'], 'pending')
        self.assertEqual(obj1_status['csv_row_index'], 1)
        self.assertEqual(obj1_status['csv_data']['colA'], 'val1')

        # Add again, including one new object
        objects_to_add_again = [
            {'unique_id': obj1_id, 'csv_row_index': 1, 'csv_data': {'colA': 'val1_new'}}, # Existing
            {'unique_id': obj3_id, 'csv_row_index': 3, 'csv_data': {'colA': 'val3'}},      # New
        ]
        added, skipped = db_handler.add_pending_objects(objects_to_add_again, self.db_path)
        self.assertEqual(added, 1, "Should add 1 new object")
        self.assertEqual(skipped, 1, "Should skip 1 existing object")
        
        obj1_status_after = db_handler.get_object_status(obj1_id, self.db_path)
        # Data for existing object should not have changed by add_pending_objects
        self.assertEqual(obj1_status_after['csv_data']['colA'], 'val1') 
        
        obj3_status = db_handler.get_object_status(obj3_id, self.db_path)
        self.assertIsNotNone(obj3_status)
        self.assertEqual(obj3_status['status'], 'pending')


    def test_get_object_status(self):
        obj_id = uuid.uuid4().hex
        csv_data_orig = {'name': 'test_obj', 'value': 123}
        db_handler.add_pending_objects([{'unique_id': obj_id, 'csv_row_index': 1, 'csv_data': csv_data_orig}], self.db_path)

        retrieved = db_handler.get_object_status(obj_id, self.db_path)
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved['unique_id'], obj_id)
        self.assertEqual(retrieved['status'], 'pending')
        self.assertEqual(retrieved['csv_data'], csv_data_orig) # Check JSON parsing

        non_existent = db_handler.get_object_status(uuid.uuid4().hex, self.db_path)
        self.assertIsNone(non_existent)

    def test_update_object_status(self):
        obj_id = uuid.uuid4().hex
        db_handler.add_pending_objects([{'unique_id': obj_id, 'csv_row_index': 1, 'csv_data': {'k': 'v'}}], self.db_path)

        update_success = db_handler.update_object_status(
            unique_id=obj_id, status='success', node_type='folder', action='sync',
            identifier='Test Folder', generated_xml='<node/>', error_message=None,
            output_batch_file='batch_1.xml', db_path=self.db_path
        )
        self.assertTrue(update_success)

        updated_obj = db_handler.get_object_status(obj_id, self.db_path)
        self.assertEqual(updated_obj['status'], 'success')
        self.assertEqual(updated_obj['node_type'], 'folder')
        self.assertEqual(updated_obj['identifier'], 'Test Folder')
        self.assertEqual(updated_obj['generated_xml'], '<node/>')
        self.assertIsNone(updated_obj['error_message'])
        self.assertEqual(updated_obj['output_batch_file'], 'batch_1.xml')
        self.assertIsInstance(updated_obj['last_attempt_timestamp'], datetime.datetime)

        # Test updating only specific fields (e.g. error message)
        db_handler.update_object_status(unique_id=obj_id, status='failed', error_message='Test Error', db_path=self.db_path)
        failed_obj = db_handler.get_object_status(obj_id, self.db_path)
        self.assertEqual(failed_obj['status'], 'failed')
        self.assertEqual(failed_obj['error_message'], 'Test Error')
        self.assertEqual(failed_obj['node_type'], 'folder') # Should remain from previous update

    def test_batch_update_object_statuses(self):
        obj1_id = uuid.uuid4().hex
        obj2_id = uuid.uuid4().hex
        obj3_id = uuid.uuid4().hex # This one won't be updated initially
        
        initial_objects = [
            {'unique_id': obj1_id, 'csv_row_index': 1, 'csv_data': {'name': 'obj1'}, 'generated_xml': '<initial_xml1/>'},
            {'unique_id': obj2_id, 'csv_row_index': 2, 'csv_data': {'name': 'obj2'}, 'generated_xml': '<initial_xml2/>'},
            {'unique_id': obj3_id, 'csv_row_index': 3, 'csv_data': {'name': 'obj3'}, 'generated_xml': '<initial_xml3/>'}
        ]
        db_handler.add_pending_objects(initial_objects, self.db_path)
        # Manually update initial XML for testing COALESCE
        for obj_data in initial_objects:
             db_handler.update_object_status(obj_data['unique_id'], 'pending', generated_xml=obj_data['generated_xml'], db_path=self.db_path)


        updates_list = [
            {
                'unique_id': obj1_id, 'status': 'success', 'node_type': 'Document', 
                'identifier': 'Doc1', 'generated_xml': '<updated_xml1/>', 
                'output_batch_file': 'b1.xml', 'error_message': None
            },
            {
                'unique_id': obj2_id, 'status': 'failed', 
                'error_message': 'Failed processing obj2',
                'generated_xml': None # Test COALESCE for generated_xml, should keep <initial_xml2/>
            },
            # obj3 is not in this batch update
        ]

        updated_count, failed_count = db_handler.batch_update_object_statuses(updates_list, self.db_path)
        self.assertEqual(updated_count, 2) 
        self.assertEqual(failed_count, 0) 

        obj1_updated = db_handler.get_object_status(obj1_id, self.db_path)
        self.assertEqual(obj1_updated['status'], 'success')
        self.assertEqual(obj1_updated['node_type'], 'Document')
        self.assertEqual(obj1_updated['identifier'], 'Doc1')
        self.assertEqual(obj1_updated['generated_xml'], '<updated_xml1/>')
        self.assertEqual(obj1_updated['output_batch_file'], 'b1.xml')
        self.assertIsNone(obj1_updated['error_message'])
        self.assertIsInstance(obj1_updated['last_attempt_timestamp'], datetime.datetime)

        obj2_updated = db_handler.get_object_status(obj2_id, self.db_path)
        self.assertEqual(obj2_updated['status'], 'failed')
        self.assertEqual(obj2_updated['error_message'], 'Failed processing obj2')
        self.assertEqual(obj2_updated['generated_xml'], '<initial_xml2/>') 
        self.assertIsInstance(obj2_updated['last_attempt_timestamp'], datetime.datetime)
        
        obj3_not_updated = db_handler.get_object_status(obj3_id, self.db_path)
        self.assertEqual(obj3_not_updated['status'], 'pending') 
        self.assertEqual(obj3_not_updated['generated_xml'], '<initial_xml3/>')


    def test_get_objects_by_status(self):
        ids = [uuid.uuid4().hex for _ in range(3)]
        db_handler.add_pending_objects([
            {'unique_id': ids[0], 'csv_row_index': 1, 'csv_data': {}},
            {'unique_id': ids[1], 'csv_row_index': 2, 'csv_data': {}},
            {'unique_id': ids[2], 'csv_row_index': 3, 'csv_data': {}},
        ], self.db_path)
        db_handler.update_object_status(ids[0], 'success', db_path=self.db_path)
        db_handler.update_object_status(ids[1], 'failed', db_path=self.db_path)

        success_items = db_handler.get_objects_by_status(['success'], self.db_path)
        self.assertEqual(len(success_items), 1)
        self.assertEqual(success_items[0]['unique_id'], ids[0])

        pending_failed = db_handler.get_objects_by_status(['pending', 'failed'], self.db_path)
        self.assertEqual(len(pending_failed), 2)
        retrieved_ids = sorted([item['unique_id'] for item in pending_failed])
        self.assertIn(ids[1], retrieved_ids)
        self.assertIn(ids[2], retrieved_ids)
        
        no_items = db_handler.get_objects_by_status(['unknown_status'], self.db_path)
        self.assertEqual(len(no_items), 0)

    def test_get_object_by_identifier(self):
        obj_id = uuid.uuid4().hex
        identifier_val = "ID_TEST_123"
        db_handler.add_pending_objects([{'unique_id': obj_id, 'csv_row_index': 1, 'csv_data': {}}], self.db_path)
        db_handler.update_object_status(obj_id, 'success', identifier=identifier_val, db_path=self.db_path)

        found_obj = db_handler.get_object_by_identifier(identifier_val, self.db_path)
        self.assertIsNotNone(found_obj)
        self.assertEqual(found_obj['unique_id'], obj_id)

        not_found_obj = db_handler.get_object_by_identifier("NON_EXISTENT_ID", self.db_path)
        self.assertIsNone(not_found_obj)

    def test_get_status_counts(self):
        ids = [uuid.uuid4().hex for _ in range(4)]
        db_handler.add_pending_objects([
            {'unique_id': ids[0], 'csv_row_index': 1, 'csv_data': {}},
            {'unique_id': ids[1], 'csv_row_index': 2, 'csv_data': {}},
            {'unique_id': ids[2], 'csv_row_index': 3, 'csv_data': {}},
            {'unique_id': ids[3], 'csv_row_index': 4, 'csv_data': {}},
        ], self.db_path)
        db_handler.update_object_status(ids[0], 'success', db_path=self.db_path)
        db_handler.update_object_status(ids[1], 'success', db_path=self.db_path)
        db_handler.update_object_status(ids[2], 'failed', db_path=self.db_path)

        counts = db_handler.get_status_counts(self.db_path)
        self.assertEqual(counts.get('success'), 2)
        self.assertEqual(counts.get('failed'), 1)
        self.assertEqual(counts.get('pending'), 1)
        self.assertIsNone(counts.get('unknown_status'))


    def test_get_file_type_counts(self):
        ids = [uuid.uuid4().hex for _ in range(3)]
        db_handler.add_pending_objects([
             {'unique_id': ids[0], 'csv_row_index': 1, 'csv_data': {}},
             {'unique_id': ids[1], 'csv_row_index': 2, 'csv_data': {}},
             {'unique_id': ids[2], 'csv_row_index': 3, 'csv_data': {}},
        ], self.db_path)
        db_handler.update_object_status(ids[0], 'success', node_type='Folder', db_path=self.db_path)
        db_handler.update_object_status(ids[1], 'success', node_type='Document', db_path=self.db_path)
        db_handler.update_object_status(ids[2], 'failed', node_type='Document', db_path=self.db_path) 

        counts = db_handler.get_file_type_counts(self.db_path)
        self.assertEqual(counts.get('Folder'), 1)
        self.assertEqual(counts.get('Document'), 1)
        self.assertEqual(len(counts), 2)


    def test_clear_database(self):
        obj_id = uuid.uuid4().hex
        db_handler.add_pending_objects([{'unique_id': obj_id, 'csv_row_index': 1, 'csv_data': {}}], self.db_path)
        self.assertTrue(db_handler.clear_database(self.db_path))
        
        status = db_handler.get_object_status(obj_id, self.db_path)
        self.assertIsNone(status)
        counts = db_handler.get_status_counts(self.db_path)
        self.assertEqual(len(counts), 0)

# --- Tests for OI_Import_Generator.py ---

class TestProcessRow(unittest.TestCase):
    def setUp(self):
        self.sample_mapping = {
            'csv_title': {'MappingType': 'Standard', 'TargetLabel': 'title', 'Category': ''},
            'csv_loc': {'MappingType': 'Standard', 'TargetLabel': 'location', 'Category': ''},
            'csv_desc': {'MappingType': 'Standard', 'TargetLabel': 'description', 'Category': ''},
            'csv_file': {'MappingType': 'Standard', 'TargetLabel': 'file', 'Category': ''},
            'csv_version': {'MappingType': 'Standard', 'TargetLabel': 'version', 'Category': ''},
            'custom_attr1': {'MappingType': 'Metadata', 'TargetLabel': 'Custom Attr 1', 'Category': 'TestCategory1'},
            'custom_attr2': {'MappingType': 'Metadata', 'TargetLabel': 'Custom Attr 2', 'Category': 'TestCategory1, TestCategory2'},
            'ignore_me': {'MappingType': 'Ignore', 'TargetLabel': 'ignored', 'Category': ''},
        }
        self.default_loc = "Default:Location"
        self.username = "testuser"
        self.category_default = "DefaultCategory"
        self.special_map = DEFAULT_SPECIAL_CHAR_MAP 
        oi_generator.global_docnum_counter = 100000 

    def test_basic_folder_creation(self):
        csv_data = {'csv_title': 'My Test Folder', 'csv_loc': 'Parent:Folder'}
        rename_list = []
        node, err = process_row(1, csv_data, self.sample_mapping, self.default_loc, self.username, 
                                "sync", "folder", self.category_default, True, None, rename_list, self.special_map)
        self.assertIsNone(err)
        self.assertIsNotNone(node)
        self.assertEqual(node.tag, "node")
        self.assertEqual(node.attrib["type"], "folder")
        self.assertEqual(node.attrib["action"], "sync")
        self.assertEqual(node.findtext("title"), "My Test Folder")
        self.assertEqual(node.findtext("location"), "Parent:Folder")
        self.assertEqual(node.findtext("createdby"), self.username) 

    def test_basic_document_creation_and_docnum(self):
        csv_data = {'csv_title': 'My Test Doc', 'csv_file': 'C:\\temp\\mydoc.pdf'}
        rename_list = []
        oi_generator.global_docnum_counter = 100000 
        
        node, err = process_row(1, csv_data, self.sample_mapping, self.default_loc, self.username,
                                "sync", "document", self.category_default, False, None, rename_list, self.special_map)
        self.assertIsNone(err)
        self.assertIsNotNone(node)
        self.assertEqual(node.attrib["type"], "document")
        self.assertEqual(node.attrib["action"], "sync")
        self.assertEqual(node.findtext("title"), "My Test Doc")
        self.assertEqual(node.findtext("file"), 'C:\\temp\\mydoc.pdf') 
        self.assertEqual(node.findtext("mimetype"), "application/x-pdf") 
        self.assertEqual(node.findtext("docnum"), str(oi_generator.global_docnum_counter)) 
        self.assertEqual(node.findtext("createdby"), self.username) 

    def test_metadata_mapping(self):
        csv_data = {
            'csv_title': 'Meta Doc', 
            'custom_attr1': 'Value1', 
            'custom_attr2': 'Value2 for two cats'
        }
        rename_list = []
        node, err = process_row(1, csv_data, self.sample_mapping, self.default_loc, self.username,
                                "sync", "document", self.category_default, True, None, rename_list, self.special_map)
        self.assertIsNone(err)
        self.assertIsNotNone(node)
        
        cat1 = node.find("category[@name='TestCategory1']")
        self.assertIsNotNone(cat1)
        self.assertEqual(cat1.find("attribute[@name='Custom Attr 1']").text, "Value1")
        self.assertEqual(cat1.find("attribute[@name='Custom Attr 2']").text, "Value2 for two cats")

        cat2 = node.find("category[@name='TestCategory2']")
        self.assertIsNotNone(cat2)
        self.assertEqual(cat2.find("attribute[@name='Custom Attr 2']").text, "Value2 for two cats")

    def test_action_update_metadata(self):
        csv_data = {'csv_title': 'Update Meta', 'csv_file': 'original.txt'}
        rename_list = []
        node, err = process_row(1, csv_data, self.sample_mapping, self.default_loc, self.username,
                                "update (metadata)", "document", self.category_default, True, None, rename_list, self.special_map)
        self.assertIsNone(err)
        self.assertIsNotNone(node)
        self.assertEqual(node.attrib["action"], "update")
        self.assertIsNone(node.find("file")) 

    def test_action_addversion(self):
        csv_data = {'csv_title': 'Add Version Doc', 'csv_loc': 'Existing:Doc', 'csv_file': 'new_version.doc', 'csv_version': '2'}
        rename_list = []
        node, err = process_row(1, csv_data, self.sample_mapping, self.default_loc, self.username,
                                "sync", "document", self.category_default, True, None, rename_list, self.special_map) 
        self.assertIsNone(err)
        self.assertIsNotNone(node)
        self.assertEqual(node.attrib["action"], "addversion")
        self.assertEqual(node.find("location").text, "Existing:Doc")
        self.assertEqual(node.find("file").text, "new_version.doc")
        self.assertEqual(node.find("version").text, "2")
        self.assertIsNone(node.find("title")) 

    def test_action_delete(self):
        csv_data = {'csv_loc': 'Existing:DocToDelete'}
        rename_list = []
        node, err = process_row(1, csv_data, self.sample_mapping, self.default_loc, self.username,
                                "delete", "document", self.category_default, True, None, rename_list, self.special_map)
        self.assertIsNone(err)
        self.assertIsNotNone(node)
        self.assertEqual(node.attrib["action"], "delete")
        self.assertEqual(node.find("location").text, "Existing:DocToDelete")
        self.assertIsNone(node.find("title"))


    def test_file_renaming_and_mimetype(self):
        csv_data = {'csv_title': 'File Test', 'csv_file': 'path/to/file:with:colons.docx'}
        rename_list = []
        node, err = process_row(1, csv_data, self.sample_mapping, self.default_loc, self.username,
                                "sync", "document", self.category_default, True, None, rename_list, self.special_map)
        self.assertIsNone(err)
        self.assertIsNotNone(node)
        expected_new_name = 'path/to/filewithcolons.docx'
        self.assertEqual(node.findtext("file"), expected_new_name)
        self.assertEqual(node.findtext("mimetype"), "application/vnd.openxmlformats-officedocument.wordprocessingml.document")
        self.assertIn(('path/to/file:with:colons.docx', expected_new_name), rename_list)

    def test_special_char_replacement(self):
        csv_data = {'csv_title': 'Title with & and ’', 'csv_desc': 'Desc with “double” quotes'}
        rename_list = []
        node, err = process_row(1, csv_data, self.sample_mapping, self.default_loc, self.username,
                                "sync", "folder", self.category_default, True, None, rename_list, self.special_map)
        self.assertIsNone(err)
        self.assertEqual(node.findtext("title"), "Title with and and '")
        self.assertEqual(node.findtext("description"), 'Desc with "double" quotes')

    def test_error_missing_action_nodetype(self):
        csv_data = {'csv_title': 'Bad Data'}
        rename_list = []
        minimal_mapping = {'csv_title': {'MappingType': 'Standard', 'TargetLabel': 'title'}}
        node, err = process_row(1, csv_data, minimal_mapping, self.default_loc, self.username,
                                "none", "none", self.category_default, True, None, rename_list, self.special_map)
        self.assertIsNone(node)
        self.assertIsNotNone(err)
        # Original error message was "Essential 'action' ('') or 'nodetype' ('') is missing or invalid based on current settings."
        # The new message due to direct check "Missing required 'action' or 'nodetype'."
        self.assertIn("Missing required 'action' or 'nodetype'", err)


    def test_use_csv_createdby(self):
        mapping_with_createdby = {**self.sample_mapping, 'csv_owner': {'MappingType': 'Standard', 'TargetLabel': 'createdby'}}
        
        csv_data1 = {'csv_title': 'Doc A', 'csv_owner': 'csvuser'}
        node1, _ = process_row(1, csv_data1, mapping_with_createdby, self.default_loc, self.username,
                               "sync", "folder", self.category_default, True, None, [], self.special_map)
        self.assertEqual(node1.find("createdby").text, "csvuser")

        csv_data2 = {'csv_title': 'Doc B', 'csv_owner': 'csvuser_ignored'}
        node2, _ = process_row(2, csv_data2, mapping_with_createdby, self.default_loc, self.username,
                               "sync", "folder", self.category_default, False, None, [], self.special_map)
        self.assertEqual(node2.find("createdby").text, self.username)

        csv_data3 = {'csv_title': 'Doc C'} 
        node3, _ = process_row(3, csv_data3, mapping_with_createdby, self.default_loc, self.username,
                               "sync", "folder", self.category_default, True, None, [], self.special_map)
        self.assertEqual(node3.find("createdby").text, self.username) 

    def test_default_location_usage(self):
        csv_data = {'csv_title': 'Doc With Default Loc'}
        mapping_no_loc = {k: v for k, v in self.sample_mapping.items() if v['TargetLabel'] != 'location'}
        
        rename_list = []
        node, err = process_row(1, csv_data, mapping_no_loc, self.default_loc, self.username,
                                "sync", "folder", self.category_default, True, None, rename_list, self.special_map)
        self.assertIsNone(err)
        self.assertIsNotNone(node)
        self.assertEqual(node.findtext("location"), self.default_loc)

    def test_default_category_usage(self):
        csv_data = {'csv_title': 'Doc With Default Cat', 'attr_for_default_cat': 'DefaultCatVal'}
        mapping_with_default_cat_meta = {
            'csv_title': {'MappingType': 'Standard', 'TargetLabel': 'title'},
            'attr_for_default_cat': {'MappingType': 'Metadata', 'TargetLabel': 'MyDefaultAttr', 'Category': ''} 
        }
        rename_list = []
        node, err = process_row(1, csv_data, mapping_with_default_cat_meta, self.default_loc, self.username,
                                "sync", "document", self.category_default, True, None, rename_list, self.special_map)
        self.assertIsNone(err)
        self.assertIsNotNone(node)
        
        cat_default_elem = node.find(f"category[@name='{self.category_default}']")
        self.assertIsNotNone(cat_default_elem, f"Default category '{self.category_default}' not found.")
        attr_elem = cat_default_elem.find("attribute[@name='MyDefaultAttr']")
        self.assertIsNotNone(attr_elem, "Attribute 'MyDefaultAttr' not found under default category.")
        self.assertEqual(attr_elem.text, "DefaultCatVal")

    def test_location_cleaning(self):
        csv_data = {'csv_title': 'Location Clean Test', 'csv_loc': 'Parent:Folder:With:Colons'}
        rename_list = []
        node, err = process_row(1, csv_data, self.sample_mapping, self.default_loc, self.username,
                                "sync", "folder", self.category_default, True, None, rename_list, self.special_map)
        self.assertIsNone(err)
        self.assertIsNotNone(node)
        self.assertEqual(node.findtext("location"), "Parent:Folder:WithColons")

        csv_data_no_colon_in_last_part = {'csv_title': 'Location Clean Test 2', 'csv_loc': 'Parent:FolderA'}
        node2, err2 = process_row(2, csv_data_no_colon_in_last_part, self.sample_mapping, self.default_loc, self.username,
                                  "sync", "folder", self.category_default, True, None, rename_list, self.special_map)
        self.assertIsNone(err2)
        self.assertEqual(node2.findtext("location"), "Parent:FolderA")

        csv_data_colon_everywhere = {'csv_title': 'Location Clean Test 3', 'csv_loc': 'A:B:C:D'}
        node3, err3 = process_row(3, csv_data_colon_everywhere, self.sample_mapping, self.default_loc, self.username,
                                  "sync", "folder", self.category_default, True, None, rename_list, self.special_map)
        self.assertIsNone(err3)
        self.assertEqual(node3.findtext("location"), "A:B:CD")

class TestApplicationUI(unittest.TestCase):
    def setUp(self):
        self.db_init_patch = patch('db_handler.init_db', return_value=True)
        self.mock_db_init = self.db_init_patch.start()

        self.app = oi_generator.Application()
        self.app.withdraw()  
        self.app.update_idletasks() 

    def tearDown(self):
        self.app.destroy()
        self.db_init_patch.stop() 

    def test_application_instantiation_via_setup(self):
        # The setUp method already creates self.app.
        # If setUp completes without error, and self.app exists,
        # then the basic instantiation worked.
        self.assertIsNotNone(self.app, "Application instance (self.app) should be created by setUp.")
        self.assertTrue(isinstance(self.app, oi_generator.Application), "self.app is not an instance of Application.")
        # No need to call destroy() here, tearDown will handle it.

    def test_ttk_theme_applied(self):
        current_theme = self.app.style.theme_use() 
        if 'clam' in self.app.style.theme_names():
             self.assertEqual(current_theme, 'clam', "TTK theme 'clam' should be active if available.")
        else:
            self.assertTrue(True, "Assuming 'clam' theme was attempted; its availability depends on the test environment.")


    @patch('os.path.exists', return_value=True) 
    @patch('builtins.open', new_callable=mock_open)
    def test_mapping_instruction_label_states(self, mock_file_open, mock_os_exists):
        self.app.csv_file.set("")
        self.app.populate_csv_mapping_tab()
        self.assertIn("Please load a CSV file", self.app.mapping_instruction_label.cget("text"))
        self.assertEqual(self.app.mapping_instruction_label.cget("foreground"), "blue")
        header_frame = self.app.csv_mapping_inner.grid_slaves(row=1)
        self.assertFalse(header_frame[0].winfo_ismapped() if header_frame else True, "Header frame should be hidden when no CSV")


        self.app.csv_file.set("dummy.csv")
        mock_file_open.return_value.read.return_value = "col1,col2\nval1,val2" 
        with patch('csv.reader', return_value=[['col1', 'col2']]) as mock_csv_reader:
            self.app.populate_csv_mapping_tab()
            mock_csv_reader.assert_called_once() 
        self.assertIn("Review and adjust the mappings below", self.app.mapping_instruction_label.cget("text"))
        self.assertEqual(self.app.mapping_instruction_label.cget("foreground"), "black")
        header_frame = self.app.csv_mapping_inner.grid_slaves(row=1)
        self.assertTrue(header_frame[0].winfo_ismapped() if header_frame else False, "Header frame should be visible")


        self.app.csv_file.set("empty.csv")
        mock_file_open.return_value.read.return_value = "" 
        with patch('csv.reader', side_effect=StopIteration) as mock_csv_reader_empty: 
            self.app.populate_csv_mapping_tab()
            mock_csv_reader_empty.assert_called_once()
        self.assertIn("appears to be empty or has no headers", self.app.mapping_instruction_label.cget("text"))
        self.assertEqual(self.app.mapping_instruction_label.cget("foreground"), "orange red")
        header_frame = self.app.csv_mapping_inner.grid_slaves(row=1)
        self.assertFalse(header_frame[0].winfo_ismapped() if header_frame else True, "Header frame should be hidden for empty CSV")


    @patch('tkinter.messagebox.showinfo') 
    def test_mapping_dirty_state_logic(self, mock_showinfo):
        self.assertFalse(self.app.mapping_dirty.get(), "Mapping should not be dirty initially.")
        self.assertEqual(self.app.mapping_status_label.cget("text"), "", "Status label should be empty initially.")

        self.app.on_mapping_changed()
        self.assertTrue(self.app.mapping_dirty.get(), "Mapping should be dirty after on_mapping_changed.")
        self.assertEqual(self.app.mapping_status_label.cget("text"), "*Unsaved changes", "Status label should show unsaved changes.")
        self.assertEqual(self.app.mapping_status_label.cget("foreground"), "red")

        self.app.save_csv_mapping_tab()
        self.assertFalse(self.app.mapping_dirty.get(), "Mapping should not be dirty after saving.")
        self.assertEqual(self.app.mapping_status_label.cget("text"), "", "Status label should be empty after saving.")
        mock_showinfo.assert_called_once() 

        self.app.on_mapping_changed() 
        self.assertTrue(self.app.mapping_dirty.get()) 
        
        with patch('os.path.exists', return_value=True), \
             patch('builtins.open', mock_open(read_data="colA,colB\n1,2")), \
             patch('csv.reader', return_value=[['colA', 'colB']]):
            self.app.csv_file.set("another_dummy.csv") 
            self.app.populate_csv_mapping_tab() 

        self.assertFalse(self.app.mapping_dirty.get(), "Mapping should not be dirty after populating new data.")
        self.assertEqual(self.app.mapping_status_label.cget("text"), "", "Status label should be empty after populating.")


    def test_settings_tab_essential_frames_created(self):
        self.app.update_idletasks() 

        children = self.app.settings_frame.winfo_children()
        frame_texts = [child.cget('text') for child in children if isinstance(child, ttk.LabelFrame)]
        
        expected_frames = [
            "Essential Project Setup",
            "Migration Type",
            "Advanced & Optional Settings"
        ]
        for expected_text in expected_frames:
            self.assertIn(expected_text, frame_texts, f"LabelFrame '{expected_text}' not found in Settings tab.")


if __name__ == '__main__':
    unittest.main(verbosity=2)
