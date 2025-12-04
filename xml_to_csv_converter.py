# This file will contain the logic to convert Object Importer/Exporter XML to CSV.

import xml.etree.ElementTree as ET
import csv
from io import StringIO
import logging

def convert_xml_to_csv(xml_string: str, selected_fields_by_node: dict | None = None) -> str:
    """
    Converts an Object Importer/Exporter XML string to a CSV formatted string.

    Args:
        xml_string: The XML data as a string.
        selected_fields_by_node: Optional. A dictionary where keys are node tags
                                 and values are lists of field names to include.
                                 If None or a tag is not present, all fields for that
                                 node are included. 'element_tag' is always included
                                 if any fields for an element are selected.

    Returns:
        A string containing the CSV data.
    """
    try:
        root = ET.fromstring(xml_string)
    except ET.ParseError as e:
        logging.error(f"Error parsing XML: {e}")
        return "Error: Could not parse XML"

    all_headers = set()
    processed_rows_data = []

    # Helper to check if a field should be included
    def should_include_field(element_tag: str, field_name: str) -> bool:
        if selected_fields_by_node is None:
            return True # Include all if no selection map
        if element_tag not in selected_fields_by_node:
            return True # Include all for this tag if not in selection map
        if not selected_fields_by_node[element_tag]: # Empty list means include none (except potentially element_tag)
            return False
        return field_name in selected_fields_by_node[element_tag]

    for element in root:
        current_row_data = {}
        element_tag_for_selection = element.tag

        # Determine if any fields are selected for this element tag
        # If selected_fields_by_node is defined and the tag is in it,
        # and the list of fields is empty, it means we should skip this element entirely,
        # unless 'element_tag' itself was the only thing "selected" (which the UI should handle).
        # For simplicity here, if a tag is in selected_fields_by_node and its list is empty,
        # no fields (not even element_tag) will be added for this row from this element.
        # The header 'element_tag' will still exist if other elements do have fields.

        selected_for_current_tag = selected_fields_by_node.get(element_tag_for_selection) if selected_fields_by_node else None

        if selected_fields_by_node is not None and element_tag_for_selection in selected_fields_by_node and not selected_fields_by_node[element_tag_for_selection]:
            # If selection is active for this tag and NO fields are selected, skip adding data for this row
            # We still add an empty dict to processed_rows_data if 'element_tag' is the only column overall,
            # or if other rows contribute data.
            # A completely empty row will be added if 'element_tag' is the ONLY selected field for this element.
            # This case is a bit tricky: if 'element_tag' is selected, it should be added.
            if 'element_tag' in selected_fields_by_node.get(element_tag_for_selection, []):
                 current_row_data['element_tag'] = element.tag
                 all_headers.add('element_tag')
            processed_rows_data.append(current_row_data) # Add potentially empty row data
            continue


        if should_include_field(element_tag_for_selection, 'element_tag'):
            current_row_data['element_tag'] = element.tag
            all_headers.add('element_tag')


        is_simple_folder_wrapper = False
        if element.tag == 'folder': # This specific structure might need careful handling with selections
            folder_children = list(element)
            if len(folder_children) == 1 and folder_children[0].tag == 'node':
                is_simple_folder_wrapper = True
                inner_node = folder_children[0]
                # For simple folder wrappers, field names are attributes of 'node' or its children
                for attr_name, attr_value in inner_node.attrib.items():
                    if should_include_field(element_tag_for_selection, attr_name):
                        all_headers.add(attr_name)
                        current_row_data[attr_name] = attr_value
                for folder_prop_child in inner_node:
                    prop_child_tag = folder_prop_child.tag
                    # Attributes of children of 'node'
                    for prop_attr_name, prop_attr_value in folder_prop_child.attrib.items():
                        header = f"{prop_child_tag}_{prop_attr_name}"
                        if should_include_field(element_tag_for_selection, header):
                            all_headers.add(header)
                            current_row_data[header] = prop_attr_value
                    # Text content of children of 'node'
                    if folder_prop_child.text and folder_prop_child.text.strip():
                        if should_include_field(element_tag_for_selection, prop_child_tag):
                            all_headers.add(prop_child_tag)
                            current_row_data[prop_child_tag] = folder_prop_child.text.strip()

        if not is_simple_folder_wrapper:
            # Direct attributes of the element
            for attr_name, attr_value in element.attrib.items():
                if should_include_field(element_tag_for_selection, attr_name):
                    all_headers.add(attr_name)
                    current_row_data[attr_name] = attr_value

            # Children of the element
            for child in element:
                child_tag = child.tag

                # Attributes of children
                for attr_name, attr_value in child.attrib.items():
                    if child_tag == 'category' and attr_name == 'name':
                        # This is part of the category structure, not a direct field
                        continue
                    elif child_tag == 'rmclassification' and attr_name == 'name':
                        # This is part of rmclassification structure
                        header = f"rmclassification_{attr_name}" # Keep original header format
                        if should_include_field(element_tag_for_selection, header):
                            all_headers.add(header)
                            current_row_data[header] = attr_value
                    elif child_tag == 'attribute' and attr_name == 'name':
                        # This is part of category structure
                        continue
                    elif child_tag == 'acl':
                        # ACLs are ignored
                        continue
                    else:
                        header = f"{child_tag}_{attr_name}"
                        if should_include_field(element_tag_for_selection, header):
                            all_headers.add(header)
                            current_row_data[header] = attr_value

                # Specific handling for complex children like 'category', 'rmclassification'
                if child_tag == 'acl':
                    pass # ACLs are ignored as per user instruction
                elif child_tag == 'category':
                    category_name_attr = child.attrib.get('name', 'UnknownCategory')
                    sane_category_name = "".join(c if c.isalnum() else '_' for c in category_name_attr)
                    found_attributes = child.findall('attribute')
                    for cat_attribute_element in found_attributes:
                        attr_name_for_header = cat_attribute_element.attrib.get('name')
                        if attr_name_for_header:
                            header = f"category_{sane_category_name}_{attr_name_for_header}"
                            if should_include_field(element_tag_for_selection, header):
                                all_headers.add(header)
                                if cat_attribute_element.text:
                                    current_row_data[header] = cat_attribute_element.text.strip()
                elif child_tag == 'rmclassification':
                    # Attributes of rmclassification itself (already handled above if 'name' was one)
                    # Children of rmclassification
                    for rm_child in child:
                        header = f"rmclassification_{rm_child.tag}"
                        if should_include_field(element_tag_for_selection, header):
                            all_headers.add(header)
                            if rm_child.text:
                                current_row_data[header] = rm_child.text.strip()
                else: # Simple child with text content
                    if child.text and child.text.strip():
                        if should_include_field(element_tag_for_selection, child_tag):
                            all_headers.add(child_tag)
                            current_row_data[child_tag] = child.text.strip()

        # Only add row if it contains some data (at least element_tag or other selected fields)
        if current_row_data:
            processed_rows_data.append(current_row_data)
        elif selected_fields_by_node is None or element_tag_for_selection not in selected_fields_by_node:
            # If no selections active for this tag, and it ended up empty, still add it (legacy behavior)
            # This can happen if an element has no attributes and no text children.
            processed_rows_data.append(current_row_data)


    if not processed_rows_data and not all_headers: # If no data rows AND no headers (e.g. empty XML or all fields deselected)
        return ""

    # If all_headers is empty but processed_rows_data is not (e.g. element_tag was the only selected field for all items)
    # this can happen if 'element_tag' was the ONLY selected field for ALL elements.
    # The UI should ideally ensure 'element_tag' is selectable.
    # If all_headers is empty because no fields were ever selected (empty selected_fields_by_node for all tags),
    # but `should_include_field` defaulted to true because selected_fields_by_node was None,
    # then all_headers might be populated by default. This logic is getting complex.
    # Let's ensure 'element_tag' is added to all_headers if it was ever intended.
    if any('element_tag' in row for row in processed_rows_data if row):
        all_headers.add('element_tag')


    # Filter out rows that are completely empty AND 'element_tag' was not a selected field for them
    # or if 'element_tag' is not even in all_headers (meaning it was never selected for any item)
    final_processed_rows = []
    if 'element_tag' in all_headers:
        for row in processed_rows_data:
            if row: # If row has any data, keep it
                final_processed_rows.append(row)
    else: # if 'element_tag' is not a header, only keep rows that have other data
        for row in processed_rows_data:
            if len(row) > 0 : # check if dict is not empty
                 final_processed_rows.append(row)

    processed_rows_data = final_processed_rows

    if not processed_rows_data and not ('element_tag' in all_headers and len(all_headers) == 1) : # if no data and headers aren't just 'element_tag'
         if not any(selected_fields_by_node.get(tag) for tag in selected_fields_by_node if selected_fields_by_node): # check if any selection was made
            return "" # If truly nothing was selected or available

    # Sort headers alphabetically for deterministic ordering
    final_headers = sorted(all_headers)

    if not final_headers and not processed_rows_data: # If after all filtering, there's nothing
        return ""
    if not final_headers and processed_rows_data: # Edge case: data but no headers (should not happen if logic is correct)
        # This might occur if only element_tag was selected and it was empty for all.
        # Or if selected_fields_by_node[tag] was empty for all tags.
        # If processed_rows_data has items, it means element_tag was populated.
        if any (row.get('element_tag') for row in processed_rows_data):
            final_headers = ['element_tag'] # fallback to at least element_tag if data exists for it
        else:
            return "" # No headers, no data with element_tag

    output = StringIO()
    writer = csv.writer(output, quoting=csv.QUOTE_ALL, lineterminator='\n')
    writer.writerow(final_headers)
    for row_data in processed_rows_data:
        # Ensure row_data is not empty and contains at least one of the final_headers
        # This check might be redundant if processed_rows_data filtering is robust
        if any(h in row_data for h in final_headers) or (not final_headers and not row_data): # allow empty row if no headers
            row_to_write = [row_data.get(header, "") for header in final_headers]
            writer.writerow(row_to_write)
        elif not row_data and 'element_tag' in final_headers and len(final_headers) == 1: # Special case for only element_tag column
            writer.writerow([""]) # Write an empty field for element_tag

    return output.getvalue()
