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

# Assuming db_handler.py and oi_import_generator.py are in the same directory or accessible via PYTHONPATH
import db_handler
from oi_import_generator import process_row, generate_default_mapping, normalize_mapping, DEFAULT_SPECIAL_CHAR_MAP
# Allow direct manipulation for testing
import oi_import_generator as oi_generator

# --- Import XML to CSV Converter ---
from xml_to_csv_converter import convert_xml_to_csv
import csv
from io import StringIO


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
        self.test_db_file = f"test_oi_status_{uuid.uuid4().hex}.db"
        self.db_path = self.test_db_file
        if os.path.exists(self.test_db_file):
            os.remove(self.test_db_file)
        self.assertTrue(db_handler.init_db(self.db_path), "Database initialization failed using file DB")
        self.conn_for_schema_check = sqlite3.connect(self.db_path)

    def tearDown(self):
        if hasattr(self, 'conn_for_schema_check') and self.conn_for_schema_check:
            self.conn_for_schema_check.close()
        if hasattr(self, 'test_db_file') and os.path.exists(self.test_db_file):
            os.remove(self.test_db_file)

    def test_init_db(self):
        with self.conn_for_schema_check: # Use the connection kept open by setUp
            cursor = self.conn_for_schema_check.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='objects';")
            self.assertIsNotNone(cursor.fetchone(), "Table 'objects' was not created.")
            cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_status';")
            self.assertIsNotNone(cursor.fetchone(), "Index 'idx_status' was not created.")
            cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_identifier';")
            self.assertIsNotNone(cursor.fetchone(), "Index 'idx_identifier' was not created.")

    def test_add_pending_objects(self):
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
        obj1_status = db_handler.get_object_status(obj1_id, self.db_path)
        self.assertIsNotNone(obj1_status)
        self.assertEqual(obj1_status['status'], 'pending')
        objects_to_add_again = [
            {'unique_id': obj1_id, 'csv_row_index': 1, 'csv_data': {'colA': 'val1_new'}},
            {'unique_id': obj3_id, 'csv_row_index': 3, 'csv_data': {'colA': 'val3'}},
        ]
        added, skipped = db_handler.add_pending_objects(objects_to_add_again, self.db_path)
        self.assertEqual(added, 1)
        self.assertEqual(skipped, 1)

    def test_get_object_status(self):
        obj_id = uuid.uuid4().hex
        csv_data_orig = {'name': 'test_obj', 'value': 123}
        db_handler.add_pending_objects([{'unique_id': obj_id, 'csv_row_index': 1, 'csv_data': csv_data_orig}], self.db_path)
        retrieved = db_handler.get_object_status(obj_id, self.db_path)
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved['csv_data'], csv_data_orig)
        self.assertIsNone(db_handler.get_object_status(uuid.uuid4().hex, self.db_path))

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

    def test_batch_update_object_statuses(self):
        ids = [uuid.uuid4().hex for _ in range(2)]
        db_handler.add_pending_objects([
            {'unique_id': ids[0], 'csv_row_index': 1, 'csv_data': {'name':'o1'}, 'generated_xml': '<o1/>'},
            {'unique_id': ids[1], 'csv_row_index': 2, 'csv_data': {'name':'o2'}, 'generated_xml': '<o2/>'}
        ], self.db_path)
        for iid in ids: db_handler.update_object_status(iid, 'pending', generated_xml=f"<{iid[:4]}/>", db_path=self.db_path)

        updates = [{'unique_id': ids[0], 'status': 'success', 'generated_xml': '<new_o1/>'}, {'unique_id': ids[1], 'status': 'failed'}]
        updated_c, failed_c = db_handler.batch_update_object_statuses(updates, self.db_path)
        self.assertEqual(updated_c, 2)
        self.assertEqual(failed_c, 0)
        self.assertEqual(db_handler.get_object_status(ids[0], self.db_path)['generated_xml'], '<new_o1/>')
        self.assertEqual(db_handler.get_object_status(ids[1], self.db_path)['generated_xml'], f"<{ids[1][:4]}/>") # Should keep original

    def test_get_objects_by_status(self):
        ids = [uuid.uuid4().hex for _ in range(3)]
        db_handler.add_pending_objects([{'unique_id': id, 'csv_row_index': i+1, 'csv_data':{}} for i,id in enumerate(ids)], self.db_path)
        db_handler.update_object_status(ids[0], 'success', db_path=self.db_path)
        db_handler.update_object_status(ids[1], 'failed', db_path=self.db_path)
        self.assertEqual(len(db_handler.get_objects_by_status(['success'], self.db_path)), 1)
        self.assertEqual(len(db_handler.get_objects_by_status(['pending', 'failed'], self.db_path)), 2)

    def test_get_object_by_identifier(self):
        obj_id = uuid.uuid4().hex; identifier = "OBJ_ID_1"
        db_handler.add_pending_objects([{'unique_id': obj_id, 'csv_row_index':1, 'csv_data':{}}], self.db_path)
        db_handler.update_object_status(obj_id, 'processing', identifier=identifier, db_path=self.db_path)
        self.assertIsNotNone(db_handler.get_object_by_identifier(identifier, self.db_path))
        self.assertIsNone(db_handler.get_object_by_identifier("XYZ", self.db_path))

    def test_get_status_counts(self):
        ids = [uuid.uuid4().hex for _ in range(3)]
        db_handler.add_pending_objects([{'unique_id': id, 'csv_row_index': i+1, 'csv_data':{}} for i,id in enumerate(ids)], self.db_path)
        db_handler.update_object_status(ids[0], 'success', db_path=self.db_path)
        db_handler.update_object_status(ids[1], 'success', db_path=self.db_path)
        db_handler.update_object_status(ids[2], 'failed', db_path=self.db_path)
        counts = db_handler.get_status_counts(self.db_path)
        self.assertEqual(counts.get('success'), 2)
        self.assertEqual(counts.get('failed'), 1)

    def test_get_file_type_counts(self):
        ids = [uuid.uuid4().hex for _ in range(2)]
        db_handler.add_pending_objects([{'unique_id': id, 'csv_row_index': i+1, 'csv_data':{}} for i,id in enumerate(ids)], self.db_path)
        db_handler.update_object_status(ids[0], 'success', node_type='TypeA', db_path=self.db_path)
        db_handler.update_object_status(ids[1], 'success', node_type='TypeB', db_path=self.db_path)
        counts = db_handler.get_file_type_counts(self.db_path)
        self.assertEqual(counts.get('TypeA'), 1)
        self.assertEqual(counts.get('TypeB'), 1)

    def test_clear_database(self):
        db_handler.add_pending_objects([{'unique_id': uuid.uuid4().hex, 'csv_row_index':1, 'csv_data':{}}], self.db_path)
        self.assertTrue(db_handler.clear_database(self.db_path))
        self.assertEqual(len(db_handler.get_status_counts(self.db_path)), 0)

class TestProcessRow(unittest.TestCase):
    def setUp(self):
        self.sample_mapping = {
            'csv_title': {'MappingType': 'Standard', 'TargetLabel': 'title', 'Category': ''},
            'csv_loc': {'MappingType': 'Standard', 'TargetLabel': 'location', 'Category': ''},
            'csv_file': {'MappingType': 'Standard', 'TargetLabel': 'file', 'Category': ''},
            'csv_version': {'MappingType': 'Standard', 'TargetLabel': 'version', 'Category': ''},
        }
        self.default_loc = "Default:Location"
        self.username = "testuser"
        self.category_default = "DefaultCategory"
        self.special_map = DEFAULT_SPECIAL_CHAR_MAP 
        oi_generator.global_docnum_counter = 100000 

    def test_basic_document_creation_and_docnum(self):
        csv_data = {'csv_title': 'My Test Doc', 'csv_file': 'C:\\temp\\mydoc.pdf'}
        node, err = process_row(1, csv_data, self.sample_mapping, self.default_loc, self.username, "sync", "document", self.category_default, False, None, [], self.special_map)
        self.assertIsNone(err)
        self.assertEqual(node.findtext("file"), 'C:/temp/mydoc.pdf')

    def test_action_update_metadata(self):
        csv_data = {'csv_title': 'Update Meta', 'csv_file': 'original.txt'}
        node, err = process_row(1, csv_data, self.sample_mapping, self.default_loc, self.username, "update (metadata)", "document", self.category_default, True, None, [], self.special_map)
        self.assertIsNone(err)
        self.assertEqual(node.attrib["action"], "update")

    def test_error_missing_action_nodetype(self):
        csv_data = {'csv_title': 'Bad Data'}
        minimal_mapping = {'csv_title': {'MappingType': 'Standard', 'TargetLabel': 'title'}}
        node, err = process_row(1, csv_data, minimal_mapping, self.default_loc, self.username, "none", "none", self.category_default, True, None, [], self.special_map)
        self.assertIsNone(node)
        self.assertIn("Missing required 'action' or 'nodetype'", err)

    def test_location_cleaning(self):
        csv_data = {'csv_loc': 'Parent:Folder:With:Colons'}
        node, _ = process_row(1, csv_data, self.sample_mapping, "", "", "sync", "folder", "", True, None, [], self.special_map)
        self.assertEqual(node.findtext("location"), "Parent:Folder:With:Colons")
        csv_data_2 = {'csv_loc': 'A:B:C:D'}
        node2, _ = process_row(1, csv_data_2, self.sample_mapping, "", "", "sync", "folder", "", True, None, [], self.special_map)
        self.assertEqual(node2.findtext("location"), "A:B:C:D")


@unittest.skipIf(not os.environ.get('DISPLAY'), "Skipping UI test in headless environment")
class TestApplicationUI(unittest.TestCase):
    def setUp(self):
        self.db_init_patch = patch('db_handler.init_db', return_value=True)
        self.mock_db_init = self.db_init_patch.start()
        self.app = oi_generator.Application()
        self.app.withdraw()  
        self.app.update_idletasks() 

    def tearDown(self):
        if hasattr(self, 'app') and self.app:
            self.app.destroy()
        self.db_init_patch.stop() 

    def test_application_instantiation_via_setup(self):
        self.assertIsNotNone(self.app)
        self.assertTrue(isinstance(self.app, oi_generator.Application))

    def test_ttk_theme_applied(self):
        if 'clam' in self.app.style.theme_names():
             self.assertEqual(self.app.style.theme_use(), 'clam')

    @patch('os.path.exists', return_value=True) 
    @patch('builtins.open', new_callable=mock_open)
    def test_mapping_instruction_label_states(self, mock_file_open, mock_os_exists):
        self.app.csv_file.set(""); self.app.populate_csv_mapping_tab()
        self.assertIn("Please load a CSV file", self.app.mapping_instruction_label.cget("text"))
        self.app.csv_file.set("dummy.csv"); mock_file_open.return_value.read.return_value = "h1,h2\nv1,v2"
        with patch('csv.reader', return_value=[['h1', 'h2']]): self.app.populate_csv_mapping_tab()
        self.assertIn("Review and adjust", self.app.mapping_instruction_label.cget("text"))

    @patch('tkinter.messagebox.showinfo') 
    def test_mapping_dirty_state_logic(self, mock_showinfo):
        self.app.on_mapping_changed(); self.assertTrue(self.app.mapping_dirty.get())
        self.app.save_csv_mapping_tab(); self.assertFalse(self.app.mapping_dirty.get())

    def test_settings_tab_essential_frames_created(self):
        self.app.update_idletasks() 
        children = self.app.settings_frame.winfo_children()
        frame_texts = [child.cget('text') for child in children if isinstance(child, ttk.LabelFrame)]
        for expected in ["Essential Project Setup", "Migration Type", "Advanced & Optional Settings"]:
            self.assertIn(expected, frame_texts)

class TestXmlToCsvConverter(unittest.TestCase):
    def setUp(self):
        self.fixture_dir = "test_fixtures"
        base_dir = os.path.dirname(os.path.abspath(__file__))
        self.sample_xml_path = os.path.join(base_dir, self.fixture_dir, "oi_example_for_csv_conversion.xml")
        os.makedirs(os.path.join(base_dir, self.fixture_dir), exist_ok=True)
        # This is the full fixture content with escaped backslashes for Python string literal
        full_fixture_content = """ <import>
        <folder><node action="create" type="folder"><location>ENTERPRISE:TESTFOLDER</location><title language="en_NZ">CPD-029931</title></node></folder>
        <folder><node action="create" type="folder"><location>ENTERPRISE:TESTFOLDER</location><title language="en_NZ">CPD-030870</title></node></folder>
        <folder><node action="create" type="folder"><location>ENTERPRISE:TESTFOLDER</location><title language="en_NZ">CPD-028947</title></node></folder>
        <node action="create" rootPathID="01" type="document">
                <acl group="Records Managers" permissions="1111111111"/>
                <acl group="Location Specific" permissions="1100000000"/>
                <acl basegroup="Business Administrators" permissions="1111111111"/>
                <acl baseowner="USER01" permissions="1111111110"/>
                <acl permissions="0000000000" standard="world"/>
                <category name="Content Server Categories:Contextual Information">
                        <attribute name="Role"><![CDATA[Advisor]]></attribute>
                        <attribute name="Branch"><![CDATA[EMPLOYMENT]]></attribute>
                </category>
                <created><![CDATA[20251318114442]]></created>
                <createdby type="0"><![CDATA[USER01]]></createdby>
                <description clear="true" language="en_NZ"/>
                <externalcreatedate><![CDATA[20251318114430]]></externalcreatedate>
                <externalidentity><![CDATA[WD\\\\USER01]]></externalidentity>
                <externalidentitytype><![CDATA[domain_userid]]></externalidentitytype>
                <externalmodifydate><![CDATA[20251318114430]]></externalmodifydate>
                <externalsource><![CDATA[file_system]]></externalsource>
                <file><![CDATA[C:\\\\Temp\\\\0000002577]]></file>
                <filename><![CDATA[Example PDF with version.pdf]]></filename>
                <filetype><![CDATA[pdf]]></filetype>
                <location><![CDATA[Enterprise:Test]]></location>
                <mime><![CDATA[application/pdf]]></mime>
                <modified><![CDATA[20251305250141]]></modified>
                <rmclassification classpath="File:Class:Documents" filenumber="DOCS.05.01" primary="true">
                        <essential><![CDATA[NON-VITAL]]></essential>
                        <official><![CDATA[0]]></official>
                        <recorddate><![CDATA[20251318]]></recorddate>
                        <rsi><![CDATA[TRANSFER 10 YEARS]]></rsi>
                        <status><![CDATA[ACTIVE]]></status>
                        <statusdate><![CDATA[20221115]]></statusdate>
                        <storage><![CDATA[HYBRID]]></storage>
                        <subject/>
                </rmclassification>
                <title language="en_NZ"><![CDATA[Example PDF with version]]></title>
        </node>
        <node action="addversion" type="document">
                <created><![CDATA[20251325092756]]></created>
                <createdby><![CDATA[USER01]]></createdby>
                <file><![CDATA[C:\\\\Temp\\\\0000002578]]></file>
                <filename><![CDATA[Example PDF with version.pdf]]></filename>
                <location><![CDATA[Enterprise:Test:Example PDF with version]]></location>
                <mime><![CDATA[application/pdf]]></mime>
        </node>
</import>"""
        with open(self.sample_xml_path, 'w', encoding='utf-8') as f_fixture:
            f_fixture.write(full_fixture_content)

        self.simple_xml_folder_node = """
<import>
    <folder>
        <node action="create" type="folder">
            <location>Test:FolderLoc</location>
            <title language="en">Test Folder Title</title>
        </node>
    </folder>
    <node action="create" type="document">
        <title>Test Doc Title</title>
        <createdby type="0">user1</createdby>
    </node>
</import>"""
        self.empty_xml = "<import></import>"
        self.malformed_xml = "<import><node>text</node></impor>"

    def test_convert_simple_xml(self):
        csv_output = convert_xml_to_csv(self.simple_xml_folder_node)
        self.assertTrue(csv_output.strip(), "CSV output should not be empty for simple XML.")
        reader = csv.reader(StringIO(csv_output))
        header = next(reader)
        self.assertIn("element_tag", header)
        self.assertIn("action", header)
        self.assertIn("type", header)
        self.assertIn("location", header)
        self.assertIn("title", header)
        self.assertIn("title_language", header)
        self.assertIn("createdby", header)
        self.assertIn("createdby_type", header)
        rows = list(reader)
        self.assertEqual(len(rows), 2, "Should be two data rows for the simple XML.")

    def test_convert_example_xml_file(self):
        if not os.path.exists(self.sample_xml_path):
             self.skipTest(f"Fixture file {self.sample_xml_path} not found.")
        with open(self.sample_xml_path, 'r', encoding='utf-8') as f:
            xml_content = f.read()
        self.assertIn("rmclassification classpath", xml_content, "Test is not using the full fixture content.")
        csv_output = convert_xml_to_csv(xml_content)
        self.assertTrue(csv_output.strip(), "CSV output should not be empty for example XML file.")
        reader = csv.reader(StringIO(csv_output))
        header = next(reader)
        expected_key_headers = [
            "element_tag", "action", "type",
            "location", "title", "title_language",
            "category_Content_Server_Categories_Contextual_Information_Role",
            "rmclassification_classpath", "rmclassification_essential"
        ]
        # ACLs are ignored, so no acl headers expected
        unexpected_acl_headers = ["acl_1_group", "acl_5_standard"]
        for h in expected_key_headers:
            self.assertIn(h, header, f"Expected header '{h}' not found.")
        for ah in unexpected_acl_headers:
            self.assertNotIn(ah, header, f"ACL header '{ah}' should not be present.")
        rows = list(reader)
        self.assertEqual(len(rows), 5, "Should be 5 data rows for the full fixture.")

    def test_empty_xml_input(self):
        csv_output = convert_xml_to_csv(self.empty_xml)
        self.assertEqual(csv_output, "", "CSV output for empty XML should be an empty string.")

    def test_malformed_xml_input(self):
        csv_output = convert_xml_to_csv(self.malformed_xml)
        self.assertTrue(csv_output.startswith("Error:"), "Malformed XML should result in an error message string.")

    def test_header_consistency_and_sorting(self):
        csv_output = convert_xml_to_csv(self.simple_xml_folder_node)
        if not csv_output.strip():
            self.fail("CSV output was empty for simple_xml_folder_node.")
        reader = csv.reader(StringIO(csv_output))
        header = next(reader)
        self.assertEqual(header, sorted(list(set(header))), "Headers should be sorted alphabetically.")

if __name__ == '__main__':
    unittest.main(verbosity=2)
