# This file will contain the logic to convert Object Importer/Exporter XML to CSV.

import xml.etree.ElementTree as ET
import csv
from io import StringIO
import logging

def convert_xml_to_csv(xml_string: str) -> str:
    """
    Converts an Object Importer/Exporter XML string to a CSV formatted string.

    Args:
        xml_string: The XML data as a string.

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
    all_headers.add('element_tag')

    for element in root:
        current_row_data = {}
        current_row_data['element_tag'] = element.tag

        is_simple_folder_wrapper = False
        if element.tag == 'folder':
            folder_children = list(element)
            if len(folder_children) == 1 and folder_children[0].tag == 'node':
                is_simple_folder_wrapper = True
                inner_node = folder_children[0]
                for attr_name, attr_value in inner_node.attrib.items():
                    all_headers.add(attr_name)
                    current_row_data[attr_name] = attr_value
                for folder_prop_child in inner_node:
                    prop_child_tag = folder_prop_child.tag
                    for prop_attr_name, prop_attr_value in folder_prop_child.attrib.items():
                        header = f"{prop_child_tag}_{prop_attr_name}"
                        all_headers.add(header)
                        current_row_data[header] = prop_attr_value
                    if folder_prop_child.text and folder_prop_child.text.strip():
                        all_headers.add(prop_child_tag)
                        current_row_data[prop_child_tag] = folder_prop_child.text.strip()

        if not is_simple_folder_wrapper:
            for attr_name, attr_value in element.attrib.items():
                all_headers.add(attr_name)
                current_row_data[attr_name] = attr_value

            acl_counter = 0
            for child in element:
                child_tag = child.tag

                for attr_name, attr_value in child.attrib.items():
                    if child_tag == 'category' and attr_name == 'name':
                        continue
                    elif child_tag == 'rmclassification' and attr_name == 'name':
                        header = f"rmclassification_{attr_name}"
                        all_headers.add(header)
                        current_row_data[header] = attr_value
                    elif child_tag == 'attribute' and attr_name == 'name':
                        continue
                    elif child_tag == 'acl':
                        continue
                    else:
                        header = f"{child_tag}_{attr_name}"
                        all_headers.add(header)
                        current_row_data[header] = attr_value

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
                            all_headers.add(header)
                            if cat_attribute_element.text:
                                current_row_data[header] = cat_attribute_element.text.strip()
                elif child_tag == 'rmclassification':
                    for rm_attr_name, rm_attr_value in child.attrib.items():
                        header = f"rmclassification_{rm_attr_name}"
                        all_headers.add(header)
                        current_row_data[header] = rm_attr_value
                    for rm_child in child:
                        header = f"rmclassification_{rm_child.tag}"
                        all_headers.add(header)
                        if rm_child.text:
                            current_row_data[header] = rm_child.text.strip()
                else:
                    if child.text and child.text.strip():
                        all_headers.add(child_tag)
                        current_row_data[child_tag] = child.text.strip()

        processed_rows_data.append(current_row_data)

    if not processed_rows_data:
        return ""

    sorted_headers = sorted(list(all_headers))
    output = StringIO()
    writer = csv.writer(output, quoting=csv.QUOTE_ALL, lineterminator='\n')
    writer.writerow(sorted_headers)
    for row_data in processed_rows_data:
        row_to_write = [row_data.get(header, "") for header in sorted_headers]
        writer.writerow(row_to_write)

    return output.getvalue()
