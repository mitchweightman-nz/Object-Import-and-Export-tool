#!/usr/bin/env python3
# Version: 2.4.1 (DB + XML Reprocess)
"""
OI Import Generator with Reprocess Tab and SQLite Backend

Enhancements:
  • Integrates SQLite database (oi_processing_status.db) for tracking object status.
  • Generates a unique ID (UUID) for each CSV row on ingest for tracking.
  • Reads CSV rows, adds them to DB as 'pending'.
  • Processing loop checks DB status ('success' rows can be skipped).
  • Updates DB status to 'processing', 'success', or 'failed' during the run.
  • Stores generated XML and error messages in the DB.
  • Reprocess tab loads failed items from _uncreated.xml (Content Server output).
  • Matches failed items to DB using identifier (title/location) to get original data.
  • Regenerates XML for failed items using original CSV data stored in DB.
  • Updates status in DB to 'reprocessed' after generating reprocess XML.
  • Standard Python logging integrated.

Requires db_handler.py in the same directory.
"""

import threading
import csv
import os
import json
import re
import xml.etree.ElementTree as ET
import tkinter as tk
from tkinter import filedialog, messagebox, ttk, simpledialog
from tkinter.ttk import Style
from datetime import datetime
import logging
import queue
import uuid

# --- Import Database Handler ---
try:
    import db_handler
except ImportError:
     logging.critical("FATAL ERROR: db_handler.py not found. Please ensure it is in the same directory.")
     exit()

# --- Import XML to CSV Converter ---
try:
    from xml_to_csv_converter import convert_xml_to_csv
except ImportError:
    logging.error("xml_to_csv_converter.py not found. XML to CSV functionality will be disabled.")
    convert_xml_to_csv = None

# -------------------- Logging Setup --------------------
# ... (logging setup code remains as it was in the original file) ...
log_queue = queue.Queue()
class TkinterLogHandler(logging.Handler):
    def __init__(self, text_widget):
        super().__init__()
        self.text_widget = text_widget
    def emit(self, record):
        msg = self.format(record)
        log_queue.put(msg + "\n")

def process_log_queue(text_widget):
    while not log_queue.empty():
        try:
            message = log_queue.get_nowait()
            if text_widget.winfo_exists():
                 text_widget.insert(tk.END, message)
                 text_widget.see(tk.END)
            else: break
        except queue.Empty: break
        except Exception as e: print(f"Error updating Tkinter log widget: {e}"); break
    if text_widget.winfo_exists():
        text_widget.after(100, lambda: process_log_queue(text_widget))

def setup_logging(log_widget):
    log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    log_level = logging.INFO; log_file = "oi_generator.log"
    file_handler = logging.FileHandler(log_file, encoding='utf-8'); file_handler.setFormatter(log_formatter); file_handler.setLevel(log_level)
    console_handler = logging.StreamHandler(); console_handler.setFormatter(log_formatter); console_handler.setLevel(log_level)
    tkinter_handler = TkinterLogHandler(log_widget); tkinter_handler.setFormatter(log_formatter); tkinter_handler.setLevel(log_level)
    logging.basicConfig(level=log_level, handlers=[file_handler, console_handler, tkinter_handler])
    logging.info("Logging initialized.");
    if log_widget.winfo_exists(): process_log_queue(log_widget)

# -------------------- Constants & Config Loading --------------------
# ... (constants and config loading functions remain as original) ...
DEFAULT_CONFIG_FILE = os.path.join(os.path.expanduser("~"), "oi_import_config.json")
def load_config_from_path(path):
    if not path or not os.path.exists(path): logging.warning(f"Config path not specified or does not exist: {path}"); return {}
    try:
        with open(path, "r", encoding="utf-8") as f: config_data = json.load(f)
        logging.info(f"Successfully loaded config from: {path}"); return config_data
    except Exception as e: logging.exception(f"Error loading config from {path}"); return {}

def save_config_to_path(config, path):
    try:
        with open(path, "w", encoding="utf-8") as f: json.dump(config, f, indent=4)
        logging.info(f"Configuration saved to: {path}")
    except Exception as e: logging.exception(f"Error saving config to {path}")

RECOGNISED_STANDARD = {"nodetype", "title", "description", "location", "created", "modified", "createdby", "createby", "action", "file", "category", "version", "docnum", "modifiedby"}
MIME_MAP = {"dwg": "application/x-acad", "arj": "application/x-arj-compressed", "tgz": "application/x-compressed", "cpio": "application/x-cpio", "csh": "application/x-csh", "dvi": "application/x-dvi", "emf": "application/x-emf", "exe": "application/x-exe", "gtar": "application/x-gtar", "gz": "application/x-gzip", "zip": "application/x-zip-compressed", "hdf": "application/x-hdf", "js": "application/x-javascript", "latex": "application/x-latex", "mif": "application/x-mif", "nc": "application/x-netcdf", "cdf": "application/x-netcdf", "msg": "application/x-outlook-msg", "pdf": "application/x-pdf", "xls": "application/x-msexcel", "ppt": "application/x-mspowerpoint", "rar": "application/x-rar-compressed", "sh": "application/x-sh", "tar": "application/x-tar", "tcl": "application/x-tcl", "tex": "application/x-tex", "texinfo": "application/x-texinfo", "tif": "image/x-tiff", "tiff": "image/x-tiff", "png": "application/x-png", "bmp": "application/x-bmp", "jpg": "image/jpeg", "jpeg": "image/jpeg", "gif": "image/gif", "avi": "video/x-msvideo", "mov": "video/x-sgi-movie", "flv": "video/x-flv", "mp3": "audio/x-mpeg", "wav": "audio/x-wav", "doc": "application/msword", "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document", "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation"}
global_docnum_counter = 100000
DEFAULT_SPECIAL_CHAR_MAP = {"&": "and", "’": "'", "“": '"', "”": '"'}
DEFAULT_CLEANSING_OPTIONS = {
    "normalize_paths": True,
    "path_colon_replacement": "",
    "clean_title_colons": True,
    "title_colon_replacement": "",
    "clean_location_colons": True,
    "location_colon_replacement": "",
    "apply_special_map": True,
}

# -------------------- Core Processing Logic (process_row etc.) --------------------
# ... (simplify_category, apply_special_char_replacements, wrap_cdata, generate_default_mapping, normalize_mapping, add_standard_elements remain as original)
def simplify_category(full_category):
    parts = full_category.split(":")
    return parts[-1].strip() if len(parts) > 1 else full_category.strip()

def apply_special_char_replacements(text, special_map, log_callback=None, field_name=None,
                                    stage="output", row_index=None):
    if not isinstance(text, str):
        return text
    cleaned_text = text
    for char, repl in special_map.items():
        cleaned_text = cleaned_text.replace(char, repl)
    if log_callback and cleaned_text != text:
        log_callback(stage, field_name or "value", text, cleaned_text, row_index,
                     "Applied special character replacements")
    return cleaned_text


def maybe_apply_special_chars(text, special_map, cleansing_options, log_callback=None,
                              field_name=None, stage="output", row_index=None):
    if not cleansing_options.get("apply_special_map", True):
        return text
    return apply_special_char_replacements(text, special_map, log_callback, field_name, stage, row_index)

def wrap_cdata(text):
    if "<![CDATA[" in text: return text
    return f"<![CDATA[{text}]]>"

def generate_default_mapping(header):
    mapping = {}
    for col in header:
        key = col.strip().lower()
        if key in RECOGNISED_STANDARD: mapping[key] = {"MappingType": "Standard", "TargetLabel": col.strip(), "Category": ""}
        else: mapping[key] = {"MappingType": "Metadata", "TargetLabel": col.strip(), "Category": ""}
    logging.info(f"Generated default mapping for {len(header)} columns.")
    return mapping

def normalize_mapping(mapping):
    norm = {}
    for key, value in mapping.items():
        norm_key = key.strip().lower()
        norm_value = {"MappingType": value.get("MappingType", "").strip(), "TargetLabel": value.get("TargetLabel", "").strip(), "Category": value.get("Category", "").strip()}
        norm[norm_key] = norm_value
    return norm

def add_standard_elements(node, std, special_map, cleansing_options, cleansing_callback=None, row_index=None):
    primary_order = ["location", "title", "description", "created", "createby", "version", "file", "mimetype", "docnum", "createdby"]
    added_keys = set()
    for key in primary_order:
        if key in std:
            val = maybe_apply_special_chars(std[key], special_map, cleansing_options, cleansing_callback, key, "output", row_index)
            if key.lower() == "createdby": ET.SubElement(node, key, attrib={"type": "0"}).text = val
            else: ET.SubElement(node, key).text = val
            added_keys.add(key)
    extra_keys = sorted(k for k in std if k not in added_keys and k.lower() not in ("action", "nodetype"))
    for key in extra_keys:
        val = maybe_apply_special_chars(std[key], special_map, cleansing_options, cleansing_callback, key, "output", row_index)
        ET.SubElement(node, key).text = val

def process_row(row_index, csv_data, mapping, default_location, username, selected_action,
                default_node_type, category_default, use_csv_createdby, report_dict,
                rename_list, special_map, cleansing_options=None, cleansing_callback=None):
    global global_docnum_counter
    if cleansing_options is None:
        cleansing_options = dict(DEFAULT_CLEANSING_OPTIONS)
    else:
        merged_options = dict(DEFAULT_CLEANSING_OPTIONS)
        merged_options.update(cleansing_options)
        cleansing_options = merged_options
    std = {}
    meta_by_cat = {}
    def log_cleaning(stage, field, original, cleaned, note):
        if cleansing_callback and original != cleaned:
            cleansing_callback(stage, field, original, cleaned, row_index, note)
    try:
        for col_csv, mapinfo in mapping.items():
            original_col_key = next((k for k in csv_data if k.strip().lower() == col_csv), None)
            if original_col_key is None: continue
            value = csv_data.get(original_col_key, "").strip()
            map_type = mapinfo.get("MappingType", "").lower()
            target_label = mapinfo.get("TargetLabel", "").strip()
            category_str = mapinfo.get("Category", "").strip()
            if map_type == "ignore": continue
            elif map_type == "standard":
                std_key = target_label.lower() if target_label.lower() in ("action", "nodetype") else target_label
                std[std_key] = value
                if category_str:
                     for cat in [c.strip() for c in category_str.split(",") if c.strip()]: meta_by_cat.setdefault(cat, {})[target_label] = value
            elif map_type == "metadata":
                cats = [c.strip() for c in category_str.split(",") if c.strip()] or [category_default]
                for cat in cats:
                    if cat: meta_by_cat.setdefault(cat, {})[target_label] = value

        if default_location and "location" not in std: std["location"] = default_location
        if not use_csv_createdby or "createdby" not in std: std["createdby"] = username
        if selected_action.lower() != "none": std["action"] = selected_action
        if default_node_type.lower() != "none": std["nodetype"] = default_node_type

        original_file = std.get("file") or std.get("filepath")
        if original_file and cleansing_options.get("normalize_paths", True):
            try:
                standardized_path = original_file.replace('\\', '/')
                normalized_path = os.path.normpath(standardized_path)
                dir_name, base_name = os.path.split(normalized_path)
                colon_replacement = cleansing_options.get("path_colon_replacement", "")
                new_base = base_name.replace(":", colon_replacement)

                if not dir_name or dir_name == '.':
                    xml_path_representation = new_base
                else:
                    dir_name_fwd = dir_name.replace(os.sep, '/')
                    xml_path_representation = f"{dir_name_fwd}/{new_base}"

                if xml_path_representation != standardized_path:
                    rename_list.append((original_file, xml_path_representation))
                    log_cleaning("input", "file path", original_file, xml_path_representation,
                                 "Normalized file path for XML compatibility")

                if "file" in std: std["file"] = xml_path_representation
                elif "filepath" in std: std["filepath"] = xml_path_representation

                ext_field = os.path.splitext(new_base)[1].lower()
                mime_type = MIME_MAP.get(ext_field.lstrip("."), "")
                if mime_type: std["mimetype"] = mime_type
                elif std.get("nodetype", "").lower() == "document": std["mimetype"] = "application/octet-stream"
            except Exception as e: logging.warning(f"Row {row_index}: Error processing file path '{original_file}': {e}")
        elif original_file:
            try:
                xml_path_representation = original_file.replace('\\', '/')
                if "file" in std: std["file"] = xml_path_representation
                elif "filepath" in std: std["filepath"] = xml_path_representation
                ext_field = os.path.splitext(os.path.basename(xml_path_representation))[1].lower()
                mime_type = MIME_MAP.get(ext_field.lstrip("."), "")
                if mime_type: std["mimetype"] = mime_type
            except Exception as e:
                logging.warning(f"Row {row_index}: Error preserving file path '{original_file}': {e}")

        action_lower = std.get("action", "").lower()
        if action_lower == "update (metadata)":
            std["action"] = "update"
            action_lower = "update"
            std.pop("file", None); std.pop("filepath", None)

        node_type_lower = std.get("nodetype", "").lower()
        if not action_lower or not node_type_lower: raise ValueError("Missing required 'action' or 'nodetype'.")

        if action_lower not in ("delete", "addversion", "update"):
            csv_version_val = std.get("version", "").strip()
            if csv_version_val.isdigit() and int(csv_version_val) > 1: std["version"] = csv_version_val; std["action"] = "addversion"; action_lower = "addversion"

        if node_type_lower == "document" and "docnum" not in std and action_lower not in ("delete", "update"): global_docnum_counter += 1; std["docnum"] = str(global_docnum_counter)
        if "title" in std and cleansing_options.get("clean_title_colons", True):
            replacement_val = cleansing_options.get("title_colon_replacement", "")
            cleaned_title = std["title"].replace(":", replacement_val)
            log_cleaning("input", "title", std["title"], cleaned_title, "Removed colons from title")
            std["title"] = cleaned_title

        if "location" in std and ":" in std["location"] and cleansing_options.get("clean_location_colons", True):
            try:
                parts = std["location"].split(':')
                if len(parts) > 1:
                    replacement_val = cleansing_options.get("location_colon_replacement", "")
                    loc_tail_cleaned = parts[-1].replace(":", replacement_val)
                    prefix_loc = ":".join(parts[:-1])
                    cleaned_location = f"{prefix_loc}:{loc_tail_cleaned}"
                    log_cleaning("input", "location", std["location"], cleaned_location,
                                 "Trimmed trailing colon characters from location")
                    std["location"] = cleaned_location
            except Exception: pass

        node_attribs = {"type": node_type_lower, "action": action_lower}
        node = ET.Element("node", attrib=node_attribs)

        if action_lower in ("addversion", "delete"):
            # ... (original logic for addversion/delete) ...
            if "location" in std:
                loc_val = maybe_apply_special_chars(std["location"], special_map, cleansing_options, cleansing_callback, "location", "output", row_index)
                ET.SubElement(node, "location", attrib={"type": "0"}).text = loc_val
            if action_lower == "addversion":
                file_val = std.get("file") or std.get("filepath")
                if file_val:
                    file_clean = maybe_apply_special_chars(file_val, special_map, cleansing_options, cleansing_callback, "file", "output", row_index)
                    ET.SubElement(node, "file", attrib={"type": "0"}).text = file_clean
                if "version" in std: ET.SubElement(node, "version").text = std["version"]
        else:
            if node_type_lower == "document" and "mimetype" not in std: std["mimetype"] = "application/octet-stream"
            add_standard_elements(node, std, special_map, cleansing_options, cleansing_callback, row_index)
            for cat_name, meta_fields in meta_by_cat.items():
                try:
                    cat_elem = ET.SubElement(node, "category", attrib={"name": cat_name})
                    for k, v in meta_fields.items():
                        attr_name = maybe_apply_special_chars(k, special_map, cleansing_options, cleansing_callback, f"{cat_name} attribute name", "output", row_index)
                        attr_val = maybe_apply_special_chars(v, special_map, cleansing_options, cleansing_callback, f"{cat_name} attribute value", "output", row_index)
                        ET.SubElement(cat_elem, "attribute", attrib={"name": attr_name}).text = attr_val
                except Exception as e: logging.error(f"Row {row_index}: Failed to add metadata category '{cat_name}': {e}")
        logging.debug(f"Row {row_index}: Successfully generated node element.")
        return node, None
    except Exception as e:
        error_msg = f"Error processing row {row_index}: {str(e)}"; logging.error(error_msg, exc_info=True); return None, error_msg

# -------------------- XML/File Generation --------------------
# ... (serialize_element, write_xml_batch, generate_rename_script remain as original) ...
def serialize_element(elem, cdata_set):
    tag = elem.tag; s = f"<{tag}"
    for attr, val in elem.attrib.items(): esc_val = val.replace('"', '&quot;'); s += f' {attr}="{esc_val}"'
    s += ">"
    if elem.text and elem.text.strip():
        txt = elem.text
        if ("*" in cdata_set) or (tag.lower() in cdata_set): s += wrap_cdata(txt)
        else: esc_txt = txt.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;'); s += esc_txt
    for child in elem: s += serialize_element(child, cdata_set)
    s += f"</{tag}>"
    if elem.tail and elem.tail.strip():
        tail = elem.tail; esc_tail = tail.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;'); s += esc_tail
    return s

def write_xml_batch(nodes_with_ids, output_path, cdata_fields):
    if not nodes_with_ids: logging.warning(f"Attempted to write empty batch to {output_path}. Skipping."); return []
    root = ET.Element("import"); valid_nodes_count = 0; processed_ids_in_batch = []
    for node_elem, unique_id in nodes_with_ids:
        if node_elem is not None: root.append(node_elem); valid_nodes_count += 1; processed_ids_in_batch.append(unique_id)
    if valid_nodes_count == 0: logging.warning(f"Batch for {output_path} contained no valid nodes. Skipping file write."); return []
    cdata_set = set();
    if cdata_fields:
        if cdata_fields.strip() == "*": cdata_set = {"*"}
        else: cdata_set = set(field.strip().lower() for field in cdata_fields.split(",") if field.strip())
    xml_string = serialize_element(root, cdata_set); xml_string = '<?xml version="1.0" encoding="utf-8"?>\n' + xml_string
    try:
        with open(output_path, "w", encoding="utf-8") as f: f.write(xml_string)
        logging.info(f"XML batch saved to: {output_path} ({valid_nodes_count} nodes)")
        return processed_ids_in_batch
    except Exception as e: logging.exception(f"Failed to write XML batch to {output_path}"); return []

def generate_rename_script(rename_list, output_dir):
    if not rename_list: logging.info("No files require renaming."); return None
    lines = []
    for original, new in rename_list:
        try:
            new_basename = os.path.basename(new); ps_original = original.replace('"', '`"'); ps_new_basename = new_basename.replace('"', '`"')
            line = f'Rename-Item -Path "{ps_original}" -NewName "{ps_new_basename}" -ErrorAction SilentlyContinue'; lines.append(line)
        except Exception as e: logging.warning(f"Could not generate rename line for '{original}' -> '{new}': {e}")
    script_text = "# Powershell script...\n" + "\n".join(lines); script_path = os.path.join(output_dir, "rename_files.ps1")
    try:
        with open(script_path, "w", encoding="utf-8") as f: f.write(script_text)
        logging.info(f"Rename script generated: {script_path}"); return script_path
    except Exception as e: logging.exception(f"Failed to write rename script {script_path}"); return None

# -------------------- Main Processing Function (DB Integrated -) --------------------
# ... (run_processing remains as original) ...
def run_processing(csv_file, xml_base, default_location, category, username,
                   mapping, action, node_type, batch_size, use_csv_createdby,
                   report_file, use_report_for_file,
                   csv_delimiter, csv_quotechar, cdata_fields, stop_flag_func=None,
                   force_reprocess=False, cleansing_callback=None, cleansing_options=None):
    logging.info("--- Starting Processing Run (DB Integrated) ---")
    logging.info(f"Force Reprocess Successful Items: {force_reprocess}")
    objects_for_db = []; original_fieldnames = []
    db_updates_batch = [] 
    try:
        with open(csv_file, "r", encoding="utf-8-sig") as f:
            sample = f.read(2048); f.seek(0); dialect = None
            if csv_delimiter:
                 class CustomDialect(csv.Dialect): delimiter = csv_delimiter; quotechar = csv_quotechar or '"'; doublequote = True; skipinitialspace = True; lineterminator = "\r\n"; quoting = csv.QUOTE_MINIMAL
                 dialect = CustomDialect()
            else:
                try: dialect = csv.Sniffer().sniff(sample, delimiters=[',', ';', '\t', '|'])
                except csv.Error: dialect = csv.excel
            reader = csv.DictReader(f, dialect=dialect); original_fieldnames = reader.fieldnames or []
            if not original_fieldnames: raise ValueError("CSV file has no header row.")
            for i, row_data in enumerate(reader):
                unique_id = uuid.uuid4().hex; objects_for_db.append({'unique_id': unique_id, 'csv_row_index': i + 1, 'csv_data': dict(row_data)})
        logging.info(f"Read {len(objects_for_db)} rows from CSV.")
    except Exception as e: logging.exception(f"Failed to read CSV file {csv_file}"); return mapping
    added_count, skipped_count = db_handler.add_pending_objects(objects_for_db)
    logging.info(f"Database sync: Added {added_count} new objects, {skipped_count} were existing.")
    if not mapping: mapping = generate_default_mapping(original_fieldnames)
    else: mapping = normalize_mapping(mapping)
    try:
        output_dir = os.path.dirname(os.path.abspath(xml_base)); base_name = os.path.splitext(os.path.basename(xml_base))[0]
        ext = os.path.splitext(xml_base)[1] or ".xml"; os.makedirs(output_dir, exist_ok=True)
    except Exception as e: logging.error(f"Invalid output path '{xml_base}': {e}"); return mapping
    report_dict = None 
    total_rows_to_process = len(objects_for_db)
    if total_rows_to_process == 0: logging.info("No data rows to process."); return mapping
    batch_size = max(1, batch_size); total_batches = (total_rows_to_process + batch_size - 1) // batch_size
    logging.info(f"Processing {total_rows_to_process} objects (estimated {total_batches} batches)...")
    rename_list = []; batch_nodes_with_ids = []; batch_count = 0; node_type_counts = {}
    processed_count = 0; skipped_count = 0; error_count = 0; current_batch_file_path = ""
    for i, db_object_info in enumerate(objects_for_db):
        unique_id = db_object_info['unique_id']; csv_data = db_object_info['csv_data']; row_num = db_object_info['csv_row_index']
        if stop_flag_func and stop_flag_func(): logging.warning(f"Stop requested. Halting before object {unique_id} (Row {row_num})."); break
        current_db_status = db_handler.get_object_status(unique_id); status_val = current_db_status.get('status') if current_db_status else 'unknown'
        if status_val == 'success' and not force_reprocess: logging.info(f"Skipping object {unique_id} (Row {row_num}): Status 'success'."); skipped_count += 1; continue
        elif status_val == 'processing': logging.warning(f"Object {unique_id} (Row {row_num}) has status 'processing'. Attempting to re-process.")
        elif status_val == 'unknown': logging.error(f"Object {unique_id} (Row {row_num}) not found in DB after initial add. Skipping."); skipped_count += 1; continue
        db_handler.update_object_status(unique_id, 'processing'); logging.debug(f"Processing object {unique_id} (Row {row_num})...")
        node_elem, error_msg = process_row(row_index=row_num, csv_data=csv_data, mapping=mapping, default_location=default_location, username=username, selected_action=action, default_node_type=node_type, category_default=category, use_csv_createdby=use_csv_createdby, report_dict=report_dict, rename_list=rename_list, special_map=DEFAULT_SPECIAL_CHAR_MAP, cleansing_options=cleansing_options or {}, cleansing_callback=cleansing_callback)
        if node_elem is not None:
            processed_count += 1; node_type_res = node_elem.attrib.get("type", "unknown"); action_res = node_elem.attrib.get("action", "unknown")
            identifier_res = node_elem.findtext("title", default="").strip() or node_elem.findtext("location", default="").strip() or f"Row_{row_num}_Object"
            generated_xml_str = ET.tostring(node_elem, encoding='unicode'); node_type_counts[node_type_res] = node_type_counts.get(node_type_res, 0) + 1
            batch_nodes_with_ids.append((node_elem, unique_id))
            db_updates_batch.append({'unique_id': unique_id, 'status': 'success', 'node_type': node_type_res, 'action': action_res, 'identifier': identifier_res, 'generated_xml': generated_xml_str, 'error_message': None, 'output_batch_file': None })
        else:
            error_count += 1
            db_updates_batch.append({'unique_id': unique_id, 'status': 'failed', 'error_message': error_msg, 'generated_xml': None })
        is_last_item = (i + 1) == total_rows_to_process
        if batch_nodes_with_ids and (len(batch_nodes_with_ids) >= batch_size or is_last_item):
            batch_count += 1
            current_batch_file_path = os.path.join(output_dir, f"{base_name}_{batch_count}{ext}")
            ids_in_batch = write_xml_batch(batch_nodes_with_ids, current_batch_file_path, cdata_fields)
            if ids_in_batch:
                for update_item in db_updates_batch:
                    if update_item['unique_id'] in ids_in_batch and update_item['status'] == 'success':
                        update_item['output_batch_file'] = current_batch_file_path
            batch_nodes_with_ids.clear() 
    if db_updates_batch:
        logging.info(f"Performing batch database update for {len(db_updates_batch)} objects...")
        updated_db_rows, failed_db_updates = db_handler.batch_update_object_statuses(db_updates_batch)
        logging.info(f"Batch DB update complete. Successfully updated rows (approx): {updated_db_rows}, Failed/Not Found: {failed_db_updates}")
        db_updates_batch.clear()
    else: logging.info("No database updates to batch process.")
    logging.info("--- Processing Run Finished ---"); logging.info(f"Total objects from CSV: {total_rows_to_process}"); logging.info(f"Successfully processed & batched for XML: {processed_count}"); logging.info(f"Skipped (due to prior success/force_reprocess=False): {skipped_count}"); logging.info(f"Processing errors: {error_count}"); logging.info(f"Total XML batches written: {batch_count}"); logging.info(f"Node type counts (for successful): {json.dumps(node_type_counts)}")
    if rename_list and not use_report_for_file:
        rename_script_path = generate_rename_script(rename_list, output_dir)
        if rename_script_path: logging.info(f"Rename script saved to: {rename_script_path}")
    elif use_report_for_file: logging.info("Using CSV report file; rename script not generated.")
    return mapping

# -------------------- Reprocess-Related Functions (Updated for XML -> DB workflow) --------------------
# ... (save_reprocessed_nodes remains as original) ...
def save_reprocessed_nodes(reprocess_data, output_path):
    root = ET.Element("import"); nodes_added = 0; processed_ids = []
    for item in reprocess_data:
        action_val = item.get('action_state', 'Skip')
        if action_val == 'Re-import':
            try:
                node_xml_str = item.get('generated_xml')
                if not node_xml_str: logging.warning(f"Skipping reprocess for {item.get('unique_id')}: Regenerated XML is missing."); continue
                node_elem = ET.fromstring(node_xml_str); root.append(node_elem); nodes_added += 1; processed_ids.append(item.get('unique_id'))
            except Exception as e: logging.exception(f"Error adding node to reprocess XML (ID: {item.get('unique_id','N/A')})")
    if nodes_added == 0: logging.warning(f"No nodes marked for 'Re-import' found. Reprocess XML not generated: {output_path}"); return [], False
    try:
        tree = ET.ElementTree(root); tree.write(output_path, encoding="utf-8", xml_declaration=True)
        logging.info(f"Reprocess XML with {nodes_added} nodes saved to: {output_path}"); return processed_ids, True
    except Exception as e: logging.exception(f"Failed to write reprocess XML to {output_path}"); return [], False

# -------------------- XML to CSV Conversion Functionality --------------------

class FieldSelectionDialog(simpledialog.Dialog):
    def __init__(self, parent, title, available_fields_by_node):
        self.available_fields_by_node = available_fields_by_node
        self.result = None
        self.checkbutton_vars = {} # Store {node_tag: {field_name: tk.BooleanVar}}
        super().__init__(parent, title)

    def body(self, master):
        master.pack(fill="both", expand=True)
        canvas = tk.Canvas(master, borderwidth=0)
        scrollbar = ttk.Scrollbar(master, orient="vertical", command=canvas.yview)
        scrollable_frame = ttk.Frame(canvas)

        scrollable_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )
        canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)

        row_idx = 0
        for node_tag, fields in sorted(self.available_fields_by_node.items()):
            if not fields: continue # Skip node tags if no fields were found (e.g. self-closing tags)

            self.checkbutton_vars[node_tag] = {}

            node_frame = ttk.LabelFrame(scrollable_frame, text=f"Fields for <{node_tag}> elements", padding=(10,5))
            node_frame.grid(row=row_idx, column=0, padx=10, pady=5, sticky="ew")
            row_idx += 1

            select_all_var = tk.BooleanVar(value=True)
            select_all_cb = ttk.Checkbutton(node_frame, text="Select/Deselect All", variable=select_all_var,
                                            command=lambda nt=node_tag, sv=select_all_var: self.toggle_all_for_node(nt, sv.get()))
            select_all_cb.grid(row=0, column=0, columnspan=2, sticky="w", pady=(0,5))

            field_idx = 0
            max_cols = 3 # Display fields in N columns
            for field_name in sorted(list(fields)):
                var = tk.BooleanVar(value=True)
                self.checkbutton_vars[node_tag][field_name] = var
                cb = ttk.Checkbutton(node_frame, text=field_name, variable=var)
                cb.grid(row=1 + field_idx // max_cols, column=field_idx % max_cols, sticky="w", padx=5)
                field_idx += 1

        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")
        return None # focus_set is handled by simpledialog.Dialog

    def toggle_all_for_node(self, node_tag, select_state):
        if node_tag in self.checkbutton_vars:
            for field_name, var in self.checkbutton_vars[node_tag].items():
                var.set(select_state)

    def apply(self):
        self.result = {}
        for node_tag, field_vars in self.checkbutton_vars.items():
            self.result[node_tag] = [field_name for field_name, var in field_vars.items() if var.get()]
        logging.info(f"Field selections made: {self.result}")

def get_all_fields_from_xml_root(xml_root):
    """
    Parses an XML root to find all unique top-level element tags and their possible fields.
    Fields include attributes and child tags (and their attributes, formatted).
    """
    available_fields_by_node = {}

    for element in xml_root: # Iterate over top-level elements (like <folder>, <document>)
        tag_name = element.tag
        if tag_name not in available_fields_by_node:
            available_fields_by_node[tag_name] = set()

        # Always consider 'element_tag' as a potential field
        available_fields_by_node[tag_name].add('element_tag')

        # Handle simple folder wrapper structure specifically for field discovery
        is_simple_folder_wrapper = False
        if element.tag == 'folder':
            folder_children = list(element)
            if len(folder_children) == 1 and folder_children[0].tag == 'node':
                is_simple_folder_wrapper = True
                inner_node = folder_children[0]
                for attr_name in inner_node.attrib:
                    available_fields_by_node[tag_name].add(attr_name)
                for folder_prop_child in inner_node:
                    prop_child_tag_name = folder_prop_child.tag
                    for prop_attr_name in folder_prop_child.attrib:
                        available_fields_by_node[tag_name].add(f"{prop_child_tag_name}_{prop_attr_name}")
                    if folder_prop_child.text and folder_prop_child.text.strip():
                         available_fields_by_node[tag_name].add(prop_child_tag_name)

        if not is_simple_folder_wrapper:
            # Direct attributes of the element
            for attr_name in element.attrib:
                available_fields_by_node[tag_name].add(attr_name)

            # Children of the element
            for child in element:
                child_tag_name = child.tag
                # Attributes of children
                for attr_name in child.attrib:
                    if child_tag_name == 'category' and attr_name == 'name': continue
                    if child_tag_name == 'rmclassification' and attr_name == 'name':
                         available_fields_by_node[tag_name].add(f"rmclassification_{attr_name}")
                         continue
                    if child_tag_name == 'attribute' and attr_name == 'name': continue
                    if child_tag_name == 'acl': continue
                    available_fields_by_node[tag_name].add(f"{child_tag_name}_{attr_name}")

                # Specific handling for complex children
                if child_tag_name == 'category':
                    category_name_attr = child.attrib.get('name', 'UnknownCategory')
                    sane_category_name = "".join(c if c.isalnum() else '_' for c in category_name_attr)
                    for cat_attr_elem in child.findall('attribute'):
                        attr_name_for_header = cat_attr_elem.attrib.get('name')
                        if attr_name_for_header:
                            available_fields_by_node[tag_name].add(f"category_{sane_category_name}_{attr_name_for_header}")
                elif child_tag_name == 'rmclassification':
                    for rm_child in child: # Children of rmclassification
                        available_fields_by_node[tag_name].add(f"rmclassification_{rm_child.tag}")
                elif child.text and child.text.strip(): # Simple child with text
                    available_fields_by_node[tag_name].add(child_tag_name)

    return available_fields_by_node


def perform_xml_to_csv_conversion(app_instance):
    if not convert_xml_to_csv:
        messagebox.showerror("Converter Error", "XML to CSV converter module is not available.")
        return
    xml_input_path = filedialog.askopenfilename(title="Select XML File to Convert", filetypes=[("XML files", "*.xml"), ("All files", "*.*")])
    if not xml_input_path: logging.info("XML to CSV conversion cancelled by user (no input file selected)."); return

    try:
        logging.info(f"Reading XML file for field discovery: {xml_input_path}")
        with open(xml_input_path, 'r', encoding='utf-8') as f_xml: xml_content = f_xml.read()

        xml_root = ET.fromstring(xml_content)
        available_fields = get_all_fields_from_xml_root(xml_root)

        if not available_fields:
            messagebox.showinfo("Info", "No processable elements found in the XML or XML is empty.")
            return

        dialog = FieldSelectionDialog(app_instance, "Select XML Fields to Convert", available_fields)
        selected_fields_by_node = dialog.result # This will be None if cancelled, or a dict if OK

        if selected_fields_by_node is None:
            logging.info("XML to CSV conversion cancelled by user (field selection dialog).")
            return

        csv_output_path = filedialog.asksaveasfilename(title="Save CSV Output As", defaultextension=".csv", filetypes=[("CSV files", "*.csv"), ("All files", "*.*")])
        if not csv_output_path: logging.info("XML to CSV conversion cancelled by user (no output file selected)."); return

        logging.info("Starting XML to CSV conversion with selected fields...")
        csv_content = convert_xml_to_csv(xml_content, selected_fields_by_node)

        if csv_content.startswith("Error:"):
            logging.error(f"Conversion failed: {csv_content}"); messagebox.showerror("Conversion Error", f"Could not convert XML to CSV:\n{csv_content}"); return

        if not csv_content.strip() and selected_fields_by_node:
             # Check if any fields were selected for any node. If selections were made but output is empty.
            is_any_field_selected = any(fields for fields in selected_fields_by_node.values())
            if is_any_field_selected:
                messagebox.showinfo("Conversion Note", "CSV conversion resulted in empty output. This might be because the selected fields do not exist in the XML data or have no values.")
            else: # No fields were selected at all
                 messagebox.showinfo("Conversion Note", "CSV conversion resulted in empty output as no fields were selected for export.")


        logging.info(f"Saving CSV output to: {csv_output_path}")
        with open(csv_output_path, 'w', encoding='utf-8', newline='') as f_csv: f_csv.write(csv_content)
        logging.info("XML to CSV conversion successful."); messagebox.showinfo("Conversion Successful", f"XML file converted and saved to:\n{csv_output_path}")

    except ET.ParseError as e:
        logging.error(f"Error parsing XML: {xml_input_path} - {e}")
        messagebox.showerror("XML Parse Error", f"Could not parse the XML file. Please check its format.\nError: {e}")
    except FileNotFoundError:
        logging.error(f"File not found: {xml_input_path}")
        messagebox.showerror("File Error", f"Input XML file not found:\n{xml_input_path}")
    except Exception as e:
        logging.exception("Error during XML to CSV conversion.")
        messagebox.showerror("Conversion Error", f"An unexpected error occurred:\n{str(e)}")

# -------------------- Tkinter Application (Updated Reprocess Logic) --------------------
class Application(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("OI Import Generator (v2.4.1 Reprocess Update)")
        self.geometry("950x650")
        style = Style(self);
        try: style.theme_use('clam')
        except tk.TclError: logging.warning("The 'clam' theme is not available, using default.")
        
        self.mapping = {}; self.csv_file = tk.StringVar(); self.xml_base = tk.StringVar()
        self.default_location = tk.StringVar(); self.category = tk.StringVar()
        self.username = tk.StringVar(value=os.getlogin()); self.mapping_file = tk.StringVar()
        self.report_file = tk.StringVar(); self.action = tk.StringVar(value="sync")
        self.node_type = tk.StringVar(value="folder"); self.batch_size = tk.IntVar(value=7000)
        self.use_csv_createdby = tk.BooleanVar(value=True); self.use_report_for_file = tk.BooleanVar(value=False)
        self.csv_delimiter = tk.StringVar(value=""); self.csv_quotechar = tk.StringVar(value="")
        self.cdata_fields = tk.StringVar(value="*")
        self.categories = ["Content Server Categories:Pītau Categories:Pītau documents", "Content Server Categories:Alternate Category:Alternate Documents"]
        self.special_char_map = dict(DEFAULT_SPECIAL_CHAR_MAP)
        self.enable_path_normalization = tk.BooleanVar(value=True)
        self.path_colon_replacement = tk.StringVar(value="")
        self.enable_title_colon_clean = tk.BooleanVar(value=True)
        self.title_colon_replacement = tk.StringVar(value="")
        self.enable_location_colon_clean = tk.BooleanVar(value=True)
        self.location_colon_replacement = tk.StringVar(value="")
        self.enable_special_char_clean = tk.BooleanVar(value=True)
        self._stop_requested = False; self._processing_thread = None
        self.current_project_path = None; self.db_available = False
        self.force_reprocess_var = tk.BooleanVar(value=False); self.mapping_dirty = tk.BooleanVar(value=False)
        self.cleansing_events = []
        self.cleansing_queue = queue.Queue()
        
        self.notebook = ttk.Notebook(self)
        self.log_frame = ttk.Frame(self.notebook)
        self.log_text = tk.Text(self.log_frame, height=15, font=("Consolas", 10), wrap=tk.WORD)
        # ... (log widget setup as original) ...
        log_scroll_y = ttk.Scrollbar(self.log_frame, orient="vertical", command=self.log_text.yview)
        log_scroll_x = ttk.Scrollbar(self.log_frame, orient="horizontal", command=self.log_text.xview)
        self.log_text.config(yscrollcommand=log_scroll_y.set, xscrollcommand=log_scroll_x.set)
        log_scroll_y.pack(side="right", fill="y"); log_scroll_x.pack(side="bottom", fill="x")
        self.log_text.pack(side="left", fill="both", expand=True, padx=(10,0), pady=10)

        setup_logging(self.log_text)
        
        self.current_db_path = db_handler.DB_PATH
        if db_handler.init_db(self.current_db_path): self.db_available = True
        else: messagebox.showerror("Database Error", "Could not initialize SQLite DB.")
        
        fallback_cfg = load_config_from_path(DEFAULT_CONFIG_FILE)
        if fallback_cfg:
            if "special_char_map" in fallback_cfg: self.special_char_map = fallback_cfg["special_char_map"]
            if "categories" in fallback_cfg: self.categories = fallback_cfg["categories"]
            self.load_config_values(fallback_cfg)
            
        self.create_widgets()
        self.create_menu() # ADDED: Call to create the menu
        
        self.refresh_categories_listbox(); self.populate_special_mapping_tab(); self.populate_csv_mapping_tab()
        self.protocol("WM_DELETE_WINDOW", self.on_closing); logging.info("Application initialized.")

    def create_menu(self): # ADDED: Menu creation method
        menubar = tk.Menu(self)
        tools_menu = tk.Menu(menubar, tearoff=0)
        tools_menu.add_command(label="Convert XML to CSV", command=lambda: perform_xml_to_csv_conversion(self))
        menubar.add_cascade(label="Tools", menu=tools_menu)
        self.config(menu=menubar)
        logging.info("Application menu created with XML to CSV tool.")

    # ... (load_config_values, gather_current_config_dict as original) ...
    def load_config_values(self, config):
        logging.debug("Loading UI values from configuration dictionary.")
        self.csv_file.set(config.get("csv_file", self.csv_file.get()))
        self.xml_base.set(config.get("xml_base", self.xml_base.get()))
        self.default_location.set(config.get("default_location", self.default_location.get()))
        self.category.set(config.get("category", self.category.get()))
        self.username.set(config.get("username", self.username.get()))
        self.mapping_file.set(config.get("mapping_file", self.mapping_file.get()))
        self.report_file.set(config.get("report_file", self.report_file.get()))
        self.action.set(config.get("action", self.action.get()))
        self.node_type.set(config.get("node_type", self.node_type.get()))
        self.batch_size.set(config.get("batch_size", self.batch_size.get()))
        self.use_csv_createdby.set(config.get("use_csv_createdby", self.use_csv_createdby.get()))
        self.use_report_for_file.set(config.get("use_report_for_file", self.use_report_for_file.get()))
        self.csv_delimiter.set(config.get("csv_delimiter", self.csv_delimiter.get()))
        self.csv_quotechar.set(config.get("csv_quotechar", self.csv_quotechar.get()))
        self.cdata_fields.set(config.get("cdata_fields", self.cdata_fields.get()))
        cleansing_cfg = config.get("cleansing_options", {})
        self.enable_path_normalization.set(cleansing_cfg.get("normalize_paths", self.enable_path_normalization.get()))
        self.path_colon_replacement.set(cleansing_cfg.get("path_colon_replacement", self.path_colon_replacement.get()))
        self.enable_title_colon_clean.set(cleansing_cfg.get("clean_title_colons", self.enable_title_colon_clean.get()))
        self.title_colon_replacement.set(cleansing_cfg.get("title_colon_replacement", self.title_colon_replacement.get()))
        self.enable_location_colon_clean.set(cleansing_cfg.get("clean_location_colons", self.enable_location_colon_clean.get()))
        self.location_colon_replacement.set(cleansing_cfg.get("location_colon_replacement", self.location_colon_replacement.get()))
        self.enable_special_char_clean.set(cleansing_cfg.get("apply_special_map", self.enable_special_char_clean.get()))
        if "csv_mapping" in config:
            self.mapping = normalize_mapping(config["csv_mapping"])
            logging.info(f"Loaded {len(self.mapping)} CSV mapping rules from config.")
    def gather_current_config_dict(self):
        logging.debug("Gathering current configuration for saving.")
        config = {
            "csv_file": self.csv_file.get(), "xml_base": self.xml_base.get(),
            "default_location": self.default_location.get(), "category": self.category.get(),
            "username": self.username.get(), "mapping_file": self.mapping_file.get(),
            "report_file": self.report_file.get(), "action": self.action.get(),
            "node_type": self.node_type.get(), "batch_size": self.batch_size.get(),
            "use_csv_createdby": self.use_csv_createdby.get(), "use_report_for_file": self.use_report_for_file.get(),
            "csv_delimiter": self.csv_delimiter.get(), "csv_quotechar": self.csv_quotechar.get(),
            "cdata_fields": self.cdata_fields.get(), "special_char_map": self.special_char_map,
            "categories": self.categories, "csv_mapping": self.mapping,
            "cleansing_options": self.get_cleansing_options()
        }
        return config

    def get_cleansing_options(self):
        return {
            "normalize_paths": self.enable_path_normalization.get(),
            "path_colon_replacement": self.path_colon_replacement.get(),
            "clean_title_colons": self.enable_title_colon_clean.get(),
            "title_colon_replacement": self.title_colon_replacement.get(),
            "clean_location_colons": self.enable_location_colon_clean.get(),
            "location_colon_replacement": self.location_colon_replacement.get(),
            "apply_special_map": self.enable_special_char_clean.get(),
        }

    # ... (create_widgets and all tab creation methods as original) ...
    def create_widgets(self):
        self.notebook.pack(fill="both", expand=True, padx=5, pady=5)
        self.settings_frame = ttk.Frame(self.notebook); self.notebook.add(self.settings_frame, text="Settings"); self.create_settings_tab(self.settings_frame)
        self.mapping_frame = ttk.Frame(self.notebook); self.notebook.add(self.mapping_frame, text="CSV Mapping"); self.create_csv_mapping_tab(self.mapping_frame)
        self.categories_frame = ttk.Frame(self.notebook); self.notebook.add(self.categories_frame, text="Categories"); self.create_categories_tab(self.categories_frame)
        self.special_frame = ttk.Frame(self.notebook); self.notebook.add(self.special_frame, text="Special Mapping"); self.create_special_mapping_tab(self.special_frame)
        self.reprocess_frame = ttk.Frame(self.notebook); self.notebook.add(self.reprocess_frame, text="Reprocess"); self.create_reprocess_tab(self.reprocess_frame)
        self.cleansing_frame = ttk.Frame(self.notebook); self.notebook.add(self.cleansing_frame, text="Data Cleansing"); self.create_cleansing_tab(self.cleansing_frame)
        self.notebook.add(self.log_frame, text="Log Output")
        self.after(200, self.flush_cleansing_queue)
    def create_settings_tab(self, frame):
        frame.columnconfigure(0, weight=1)
        essential_frame = ttk.LabelFrame(frame, text="Essential Project Setup", padding=(10, 5)); essential_frame.grid(row=0, column=0, sticky="ew", padx=10, pady=(5, 7)); essential_frame.columnconfigure(1, weight=1)
        ttk.Label(essential_frame, text="CSV Input File:").grid(row=0, column=0, sticky="w", pady=2, padx=5); ttk.Entry(essential_frame, textvariable=self.csv_file, width=60).grid(row=0, column=1, sticky="ew", pady=2); ttk.Button(essential_frame, text="Browse...", command=self.browse_csv).grid(row=0, column=2, padx=5, pady=2)
        ttk.Button(essential_frame, text="Load CSV Header -> Mapping Tab", command=self.populate_csv_mapping_tab).grid(row=1, column=1, sticky="w", pady=(2,10), padx=5)
        ttk.Label(essential_frame, text="Output XML (base name):").grid(row=2, column=0, sticky="w", pady=2, padx=5); ttk.Entry(essential_frame, textvariable=self.xml_base, width=60).grid(row=2, column=1, sticky="ew", pady=2); ttk.Button(essential_frame, text="Browse...", command=self.browse_xml).grid(row=2, column=2, padx=5, pady=2)
        ttk.Label(essential_frame, text="Default Location Prefix:").grid(row=3, column=0, sticky="w", pady=2, padx=5); ttk.Entry(essential_frame, textvariable=self.default_location, width=60).grid(row=3, column=1, sticky="ew", pady=2)
        ttk.Label(essential_frame, text="Default Category (if unmapped):").grid(row=4, column=0, sticky="w", pady=(2,5), padx=5); ttk.Entry(essential_frame, textvariable=self.category, width=60).grid(row=4, column=1, sticky="ew", pady=(2,5))
        migration_frame = ttk.LabelFrame(frame, text="Migration Type", padding=(10, 5)); migration_frame.grid(row=1, column=0, sticky="ew", padx=10, pady=(0, 7)); migration_frame.columnconfigure(1, weight=1)
        ttk.Label(migration_frame, text="Action Override:").grid(row=0, column=0, sticky="w", pady=2, padx=5); action_radio_frame = ttk.Frame(migration_frame) ; action_radio_frame.grid(row=0, column=1, sticky="w", pady=2)
        for a in ["none", "sync", "addversion", "delete", "update (metadata)"]: ttk.Radiobutton(action_radio_frame, text=a.capitalize(), variable=self.action, value=a).pack(side="left", padx=5)
        self.action.set("sync") 
        ttk.Label(migration_frame, text="Default Node Type Override:").grid(row=1, column=0, sticky="w", pady=(2,5), padx=5); type_radio_frame = ttk.Frame(migration_frame) ; type_radio_frame.grid(row=1, column=1, sticky="w", pady=(2,5))
        for nt in ["none", "folder", "document"]: ttk.Radiobutton(type_radio_frame, text=nt.capitalize(), variable=self.node_type, value=nt).pack(side="left", padx=5)
        self.node_type.set("folder") 
        advanced_frame_main = ttk.LabelFrame(frame, text="Advanced & Optional Settings", padding=(10, 5)); advanced_frame_main.grid(row=2, column=0, sticky="ew", padx=10, pady=(0, 7)); advanced_frame_main.columnconfigure(1, weight=1)
        ttk.Label(advanced_frame_main, text="CSV Report File (Optional):").grid(row=0, column=0, sticky="w", pady=2, padx=5); ttk.Entry(advanced_frame_main, textvariable=self.report_file, width=60).grid(row=0, column=1, sticky="ew", pady=2); ttk.Button(advanced_frame_main, text="Browse...", command=self.browse_report).grid(row=0, column=2, padx=5, pady=2)
        ttk.Label(advanced_frame_main, text="Created By Override:").grid(row=1, column=0, sticky="w", pady=2, padx=5); ttk.Entry(advanced_frame_main, textvariable=self.username, width=60).grid(row=1, column=1, sticky="w", pady=2)
        ttk.Label(advanced_frame_main, text="Batch Size (rows per XML):").grid(row=2, column=0, sticky="w", pady=2, padx=5); ttk.Entry(advanced_frame_main, textvariable=self.batch_size, width=10).grid(row=2, column=1, sticky="w", pady=2)
        check_frame = ttk.Frame(advanced_frame_main) ; check_frame.grid(row=3, column=0, columnspan=3, sticky="w", padx=5, pady=5)
        ttk.Checkbutton(check_frame, text="Use 'createdby' column from CSV (if mapped)", variable=self.use_csv_createdby).pack(side="left", padx=5); ttk.Checkbutton(check_frame, text="Use CSV Report File for <file> path", variable=self.use_report_for_file).pack(side="left", padx=5); ttk.Checkbutton(check_frame, text="Force Reprocess Successful Items", variable=self.force_reprocess_var).pack(side="left", padx=5)
        advanced_csv_options_content_frame = ttk.LabelFrame(advanced_frame_main, text="Advanced CSV Parsing", padding=(10,5)) ; advanced_csv_options_content_frame.grid(row=4, column=0, columnspan=3, sticky="ew", padx=5, pady=(10,5)) ; advanced_csv_options_content_frame.columnconfigure(1, weight=1)
        ttk.Label(advanced_csv_options_content_frame, text="CSV Delimiter (blank=auto):").grid(row=0, column=0, sticky="w", pady=2, padx=5); ttk.Entry(advanced_csv_options_content_frame, textvariable=self.csv_delimiter, width=5).grid(row=0, column=1, sticky="w", pady=2)
        ttk.Label(advanced_csv_options_content_frame, text="CSV Quote Char (blank=auto):").grid(row=1, column=0, sticky="w", pady=2, padx=5); ttk.Entry(advanced_csv_options_content_frame, textvariable=self.csv_quotechar, width=5).grid(row=1, column=1, sticky="w", pady=2)
        ttk.Label(advanced_csv_options_content_frame, text="CDATA Fields (',' sep, '*' all):").grid(row=2, column=0, sticky="w", pady=(2,5), padx=5) ; ttk.Entry(advanced_csv_options_content_frame, textvariable=self.cdata_fields, width=60).grid(row=2, column=1, sticky="ew", pady=(2,5))
        action_btn_frame = ttk.Frame(frame, padding=(10, 10)) ; action_btn_frame.grid(row=3, column=0, sticky="ew", padx=10, pady=(5,0)) ; action_btn_frame.columnconfigure(0, weight=1); action_btn_frame.columnconfigure(1, weight=1); action_btn_frame.columnconfigure(2, weight=1)
        ttk.Button(action_btn_frame, text="Start Generation", command=self.start_generation, width=20).grid(row=0, column=0, padx=10, pady=5, sticky="e") 
        self.stop_button = ttk.Button(action_btn_frame, text="Stop Generation", command=self.stop_generation, width=20, state=tk.DISABLED); self.stop_button.grid(row=0, column=1, padx=10, pady=5, sticky="")
        self.report_button = ttk.Button(action_btn_frame, text="View Status Report", command=self.view_status_report, width=20, state=tk.NORMAL if self.db_available else tk.DISABLED); self.report_button.grid(row=0, column=2, padx=10, pady=5, sticky="w")
        project_btn_frame = ttk.Frame(frame, padding=(10, 5)) ; project_btn_frame.grid(row=4, column=0, sticky="ew", padx=10, pady=(0,5)) ; project_btn_frame.columnconfigure(0, weight=1); project_btn_frame.columnconfigure(1, weight=1)
        ttk.Button(project_btn_frame, text="Open Project...", command=self.open_project, width=20).grid(row=0, column=0, padx=10, pady=5, sticky="e"); ttk.Button(project_btn_frame, text="Save Project...", command=self.save_project, width=20).grid(row=0, column=1, padx=10, pady=5, sticky="w")
    def create_csv_mapping_tab(self, frame): # as original
        top_frame = ttk.Frame(frame, padding=(10, 5)); top_frame.pack(fill="x")
        ttk.Button(top_frame, text="Load CSV Header", command=self.populate_csv_mapping_tab).pack(side="left", padx=5)
        ttk.Button(top_frame, text="Save Column Mappings", command=self.save_csv_mapping_tab).pack(side="left", padx=5) 
        self.mapping_status_label = ttk.Label(top_frame, text="", foreground="red") ; self.mapping_status_label.pack(side="left", padx=10, pady=2)
        self.mapping_dirty.trace_add('write', lambda *args: self.update_mapping_status_label())
        canvas_frame = ttk.Frame(frame); canvas_frame.pack(fill="both", expand=True, padx=10, pady=(5, 10)) 
        self.csv_mapping_canvas = tk.Canvas(canvas_frame); self.csv_mapping_scroll = ttk.Scrollbar(canvas_frame, orient="vertical", command=self.csv_mapping_canvas.yview); self.csv_mapping_inner = ttk.Frame(self.csv_mapping_canvas)
        self.csv_mapping_canvas.configure(yscrollcommand=self.csv_mapping_scroll.set); self.csv_mapping_scroll.pack(side="right", fill="y"); self.csv_mapping_canvas.pack(side="left", fill="both", expand=True)
        self.csv_mapping_canvas_window = self.csv_mapping_canvas.create_window((0, 0), window=self.csv_mapping_inner, anchor="nw")
        self.csv_mapping_inner.bind("<Configure>", self._on_mapping_configure); self.csv_mapping_canvas.bind("<Configure>", self._on_canvas_configure)
        self.mapping_instruction_label = ttk.Label(self.csv_mapping_inner, text="") ; self.mapping_instruction_label.grid(row=0, column=0, sticky="ew", padx=5, pady=5)
        header_frame = ttk.Frame(self.csv_mapping_inner); header_frame.grid(row=1, column=0, sticky="ew", pady=(0,2)) 
        ttk.Label(header_frame, text="CSV Column", borderwidth=1, relief="solid", anchor="center").grid(row=0, column=0, sticky="ew", padx=1, pady=1); ttk.Label(header_frame, text="Mapping Type", borderwidth=1, relief="solid", anchor="center").grid(row=0, column=1, sticky="ew", padx=1, pady=1); ttk.Label(header_frame, text="Target Label", borderwidth=1, relief="solid", anchor="center").grid(row=0, column=2, sticky="ew", padx=1, pady=1); ttk.Label(header_frame, text="Category", borderwidth=1, relief="solid", anchor="center").grid(row=0, column=3, sticky="ew", padx=1, pady=1); ttk.Label(header_frame, text="Select Cat.", borderwidth=1, relief="solid", anchor="center").grid(row=0, column=4, sticky="ew", padx=1, pady=1)
        header_frame.columnconfigure(0, weight=3); header_frame.columnconfigure(1, weight=2); header_frame.columnconfigure(2, weight=3); header_frame.columnconfigure(3, weight=3); header_frame.columnconfigure(4, weight=1)
        self.csv_mapping_entries = []; self.csv_mapping_rows_frame = ttk.Frame(self.csv_mapping_inner); self.csv_mapping_rows_frame.grid(row=2, column=0, sticky="ew")
        self.csv_mapping_rows_frame.columnconfigure(0, weight=3); self.csv_mapping_rows_frame.columnconfigure(1, weight=2); self.csv_mapping_rows_frame.columnconfigure(2, weight=3); self.csv_mapping_rows_frame.columnconfigure(3, weight=3); self.csv_mapping_rows_frame.columnconfigure(4, weight=1)
    def _on_mapping_configure(self, event): self.csv_mapping_canvas.configure(scrollregion=self.csv_mapping_canvas.bbox("all")) # as original
    def _on_canvas_configure(self, event): canvas_width = event.width; self.csv_mapping_canvas.itemconfig(self.csv_mapping_canvas_window, width=canvas_width) # as original
    def populate_csv_mapping_tab(self): # as original (with correct dialect handling)
        for widget in self.csv_mapping_rows_frame.winfo_children(): widget.destroy()
        self.csv_mapping_entries.clear(); header_children = self.csv_mapping_inner.grid_slaves(row=1) ; header_frame_widget = header_children[0] if header_children else None; csv_path = self.csv_file.get()
        if not csv_path or not os.path.exists(csv_path): self.mapping_instruction_label.config(text="Please load a CSV file from the 'Settings' tab to view and configure column mappings.", foreground="blue"); (header_frame_widget.grid_remove() if header_frame_widget else None); self.mapping_dirty.set(False); return
        self.mapping_instruction_label.config(text="Review and adjust the mappings below. Click 'Save Column Mappings' when done.", foreground="black"); (header_frame_widget.grid() if header_frame_widget else None)
        try:
            with open(csv_path, "r", encoding="utf-8-sig") as f: sample = f.read(2048); f.seek(0); dialect = None
            if self.csv_delimiter.get(): dialect = type('CustomDialect', (csv.Dialect,), {'delimiter': self.csv_delimiter.get(), 'quotechar': self.csv_quotechar.get() or '"', 'doublequote': True, 'skipinitialspace': True, 'lineterminator': "\r\n", 'quoting': csv.QUOTE_MINIMAL})()
            else:
                try: dialect = csv.Sniffer().sniff(sample, delimiters=[',', ';', '\t', '|'])
                except csv.Error: dialect = csv.excel
            reader = csv.reader(f, dialect=dialect); headers = next(reader)
        except StopIteration: self.mapping_instruction_label.config(text=f"CSV file '{os.path.basename(csv_path)}' appears to be empty or has no headers.", foreground="orange red"); (header_frame_widget.grid_remove() if header_frame_widget else None); self.mapping_dirty.set(False); return
        except Exception as e: self.mapping_instruction_label.config(text=f"Error reading CSV: {e}", foreground="red"); (header_frame_widget.grid_remove() if header_frame_widget else None); logging.exception(f"Error reading CSV header from {csv_path}"); self.mapping_dirty.set(False); return
        row_idx = 0
        for col_header in headers:
            original_col = col_header.strip(); norm_col = original_col.lower(); default_map = self.mapping.get(norm_col, {"MappingType": "Standard" if norm_col in RECOGNISED_STANDARD else "Metadata", "TargetLabel": original_col, "Category": ""})
            lbl_col = ttk.Label(self.csv_mapping_rows_frame, text=original_col, anchor="w"); lbl_col.grid(row=row_idx, column=0, sticky="ew", padx=5, pady=1)
            cmb_map_type = ttk.Combobox(self.csv_mapping_rows_frame, values=["Ignore", "Standard", "Metadata"], width=12, state="readonly"); cmb_map_type.set(default_map.get("MappingType", "Metadata")); cmb_map_type.grid(row=row_idx, column=1, sticky="ew", padx=5, pady=1); cmb_map_type.bind("<<ComboboxSelected>>", self.on_mapping_changed)
            ent_target = ttk.Entry(self.csv_mapping_rows_frame, width=20); ent_target.insert(0, default_map.get("TargetLabel", original_col)); ent_target.grid(row=row_idx, column=2, sticky="ew", padx=5, pady=1); ent_target.bind("<KeyRelease>", self.on_mapping_changed)
            cat_var = tk.StringVar(); cat_var.set(default_map.get("Category", "")); cat_var.trace_add('write', self.on_mapping_changed)
            cat_display_entry = ttk.Entry(self.csv_mapping_rows_frame, textvariable=cat_var, state="readonly", width=18); cat_display_entry.grid(row=row_idx, column=3, sticky="ew", padx=5, pady=1)
            original_cat_val = cat_var.get() ; select_btn = ttk.Button(self.csv_mapping_rows_frame, text="...", width=3, command=lambda v=cat_var, ov=original_cat_val: self.open_category_selector(v, ov)); select_btn.grid(row=row_idx, column=4, sticky="w", padx=(0,5), pady=1)
            self.csv_mapping_entries.append((norm_col, cmb_map_type, ent_target, cat_var)); row_idx += 1
        self.csv_mapping_canvas.update_idletasks(); self.csv_mapping_canvas.configure(scrollregion=self.csv_mapping_canvas.bbox("all")); self.mapping_dirty.set(False)
    def save_csv_mapping_tab(self): # as original
        new_mapping = {};
        for norm_col, cmb_map_type, ent_target, cat_var in self.csv_mapping_entries: mtype = cmb_map_type.get().strip(); target = ent_target.get().strip(); cat = cat_var.get().strip(); (new_mapping[norm_col] if mtype and target else logging.warning(f"Mapping ignored for CSV column '{norm_col}' due to missing Type or Target Label." if mtype != "Ignore" else None)) # this line is wrong
        for norm_col, cmb_map_type, ent_target, cat_var in self.csv_mapping_entries:
            mtype = cmb_map_type.get().strip(); target = ent_target.get().strip(); cat = cat_var.get().strip()
            if mtype and target: new_mapping[norm_col] = {"MappingType": mtype, "TargetLabel": target, "Category": cat}
            elif mtype != "Ignore": logging.warning(f"Mapping ignored for CSV column '{norm_col}' due to missing Type or Target Label.")
        self.mapping = new_mapping; self.mapping_dirty.set(False); logging.info(f"CSV mapping updated and saved internally ({len(self.mapping)} rules)."); messagebox.showinfo("Mapping Saved", "Column mapping rules have been updated.\nRemember to save the project to persist these changes across sessions.")
    def open_category_selector(self, cat_var, original_value_for_var): # as original
        win = tk.Toplevel(self); win.title("Select Categories"); win.geometry("400x400"); win.transient(self); win.grab_set() 
        instruction_frame = ttk.Frame(win, padding=(10,5)); instruction_frame.pack(fill="x"); ttk.Label(instruction_frame, text="Select categories if this column maps to specific Content Server categories.\nThis is typically used for 'Metadata' mapping types.", justify=tk.LEFT).pack(anchor="w")
        list_frame = ttk.Frame(win); list_frame.pack(fill="both", expand=True, padx=10, pady=5); cat_listbox = tk.Listbox(list_frame, selectmode=tk.MULTIPLE, exportselection=False)
        cat_scroll = ttk.Scrollbar(list_frame, orient="vertical", command=cat_listbox.yview); cat_listbox.config(yscrollcommand=cat_scroll.set); cat_scroll.pack(side="right", fill="y"); cat_listbox.pack(side="left", fill="both", expand=True)
        self.category_display_map = {simplify_category(c): c for c in self.categories}; display_categories = sorted(self.category_display_map.keys())
        for item in display_categories: cat_listbox.insert(tk.END, item)
        current_full_paths = [s.strip() for s in cat_var.get().split(",") if s.strip()]; current_simple_names = [simplify_category(fp) for fp in current_full_paths]
        for idx, simple_name in enumerate(display_categories):
            if simple_name in current_simple_names: cat_listbox.selection_set(idx)
        btn_frame = ttk.Frame(win); btn_frame.pack(pady=10)
        def on_ok(): selected_indices = cat_listbox.curselection(); selected_full_paths = [self.category_display_map[display_categories[i]] for i in selected_indices]; new_value = ",".join(selected_full_paths); (cat_var.set(new_value) if new_value != original_value_for_var else None); win.destroy()
        def on_cancel(): win.destroy()
        ttk.Button(btn_frame, text="OK", command=on_ok, width=10).pack(side="left", padx=10); ttk.Button(btn_frame, text="Cancel", command=on_cancel, width=10).pack(side="left", padx=10); win.wait_window()
    def create_special_mapping_tab(self, frame): # as original
        top_frame = ttk.Frame(frame, padding=(10, 5)); top_frame.pack(fill="x"); ttk.Button(top_frame, text="Add New Row", command=self.add_special_mapping_row).pack(side="left", padx=5); ttk.Button(top_frame, text="Apply && Save Special Mapping", command=self.save_special_mapping_tab).pack(side="left", padx=5); ttk.Button(top_frame, text="Remove Selected Rows", command=self.remove_special_mapping_rows).pack(side="right", padx=5)
        columns = ("char", "replacement"); self.special_tree = ttk.Treeview(frame, columns=columns, show="headings", selectmode="extended"); self.special_tree.heading("char", text="Special Character"); self.special_tree.heading("replacement", text="Replacement Text"); self.special_tree.column("char", width=150, anchor="center"); self.special_tree.column("replacement", width=250, anchor="w")
        tree_scroll = ttk.Scrollbar(frame, orient="vertical", command=self.special_tree.yview); self.special_tree.configure(yscrollcommand=tree_scroll.set); tree_scroll.pack(side="right", fill="y", padx=(0,10), pady=(0,10)); self.special_tree.pack(fill="both", expand=True, padx=(10,0), pady=(0,10)); self.special_tree.bind("<Double-1>", self.on_special_tree_double_click); self.populate_special_mapping_tab()
    def create_cleansing_tab(self, frame):
        top_frame = ttk.Frame(frame, padding=(10, 5)); top_frame.pack(fill="x")
        ttk.Label(top_frame, text="Choose what cleansing to apply; every change still logs below when enabled.").pack(side="left")
        ttk.Button(top_frame, text="Clear Cleansing Entries", command=self.clear_cleansing_log).pack(side="right")

        options_frame = ttk.LabelFrame(frame, text="Cleansing Controls", padding=(10, 5))
        options_frame.pack(fill="x", padx=10, pady=(0, 10))

        ttk.Checkbutton(options_frame, text="Normalize file paths (slashes + colon replacement)", variable=self.enable_path_normalization).grid(row=0, column=0, sticky="w")
        ttk.Label(options_frame, text="Colon replacement (file name):").grid(row=0, column=1, sticky="e", padx=(10,2))
        ttk.Entry(options_frame, textvariable=self.path_colon_replacement, width=10).grid(row=0, column=2, sticky="w")

        ttk.Checkbutton(options_frame, text="Clean title colons", variable=self.enable_title_colon_clean).grid(row=1, column=0, sticky="w", pady=2)
        ttk.Label(options_frame, text="Title colon replacement:").grid(row=1, column=1, sticky="e", padx=(10,2))
        ttk.Entry(options_frame, textvariable=self.title_colon_replacement, width=10).grid(row=1, column=2, sticky="w")

        ttk.Checkbutton(options_frame, text="Clean trailing location colons", variable=self.enable_location_colon_clean).grid(row=2, column=0, sticky="w", pady=2)
        ttk.Label(options_frame, text="Location colon replacement:").grid(row=2, column=1, sticky="e", padx=(10,2))
        ttk.Entry(options_frame, textvariable=self.location_colon_replacement, width=10).grid(row=2, column=2, sticky="w")

        ttk.Checkbutton(options_frame, text="Apply special character map", variable=self.enable_special_char_clean).grid(row=3, column=0, sticky="w", pady=2)
        ttk.Button(options_frame, text="Edit map...", command=lambda: self.notebook.select(self.special_frame)).grid(row=3, column=1, sticky="e", padx=(10,2))
        ttk.Label(options_frame, text="(Uses rows in Special Mapping tab)").grid(row=3, column=2, sticky="w")

        for i in range(3):
            options_frame.columnconfigure(i, weight=1)

        columns = ("timestamp", "stage", "row", "field", "original", "cleaned", "details")
        self.cleansing_tree = ttk.Treeview(frame, columns=columns, show="headings", selectmode="extended")
        headings = {
            "timestamp": "Time",
            "stage": "Stage",
            "row": "Row",
            "field": "Field",
            "original": "Original",
            "cleaned": "Cleaned",
            "details": "Details",
        }
        widths = {"timestamp": 90, "stage": 80, "row": 60, "field": 140, "original": 220, "cleaned": 220, "details": 240}
        for col in columns:
            self.cleansing_tree.heading(col, text=headings[col])
            self.cleansing_tree.column(col, width=widths.get(col, 120), anchor="w", stretch=(col in {"original", "cleaned", "details"}))

        tree_scroll_y = ttk.Scrollbar(frame, orient="vertical", command=self.cleansing_tree.yview)
        tree_scroll_x = ttk.Scrollbar(frame, orient="horizontal", command=self.cleansing_tree.xview)
        self.cleansing_tree.configure(yscrollcommand=tree_scroll_y.set, xscrollcommand=tree_scroll_x.set)
        tree_scroll_y.pack(side="right", fill="y", padx=(0,10), pady=(0,10))
        tree_scroll_x.pack(side="bottom", fill="x", padx=(10,10), pady=(0,10))
        self.cleansing_tree.pack(fill="both", expand=True, padx=(10,0), pady=(0,10))

    def clear_cleansing_log(self):
        self.cleansing_events.clear()
        while not self.cleansing_queue.empty():
            try:
                self.cleansing_queue.get_nowait()
            except queue.Empty:
                break
        if hasattr(self, "cleansing_tree"):
            for item_id in self.cleansing_tree.get_children():
                self.cleansing_tree.delete(item_id)

    def record_cleansing_action(self, stage, field, original, cleaned, row_index=None, details=""):
        timestamp = datetime.now().strftime("%H:%M:%S")
        log_message = f"[Cleansing-{stage}] Row {row_index if row_index is not None else '-'} {field}: '{original}' -> '{cleaned}'"
        if details:
            log_message += f" ({details})"
        logging.info(log_message)
        event = {
            "timestamp": timestamp,
            "stage": stage.capitalize(),
            "row": row_index if row_index is not None else "-",
            "field": field,
            "original": original,
            "cleaned": cleaned,
            "details": details,
        }
        try:
            self.cleansing_queue.put(event)
        except Exception as e:
            logging.debug(f"Unable to queue cleansing event: {e}")

    def flush_cleansing_queue(self):
        if not hasattr(self, "cleansing_tree") or not self.cleansing_tree.winfo_exists():
            return
        try:
            while not self.cleansing_queue.empty():
                event = self.cleansing_queue.get_nowait()
                self.cleansing_events.append(event)
                self.cleansing_tree.insert("", "end", values=(event["timestamp"], event["stage"], event["row"], event["field"], event["original"], event["cleaned"], event["details"]))
        except queue.Empty:
            pass
        finally:
            self.after(400, self.flush_cleansing_queue)
    def populate_special_mapping_tab(self): # as original
        for item in self.special_tree.get_children(): self.special_tree.delete(item)
        for char, replacement in sorted(self.special_char_map.items()): self.special_tree.insert("", "end", values=(char, replacement))
    def add_special_mapping_row(self): self.special_tree.focus(self.special_tree.insert("", "end", values=("", ""))) # as original
    def remove_special_mapping_rows(self): # as original
        selected_items = self.special_tree.selection()
        if not selected_items: messagebox.showwarning("No Selection", "Please select row(s) to remove."); return
        if messagebox.askyesno("Confirm Removal", f"Are you sure you want to remove {len(selected_items)} selected mapping(s)?"):
            for item_id in selected_items: self.special_tree.delete(item_id)
    def on_special_tree_double_click(self, event): # as original
         region = self.special_tree.identify("region", event.x, event.y); col_id = self.special_tree.identify_column(event.x); row_id = self.special_tree.identify_row(event.y)
         if region == "cell" and row_id: self.edit_special_cell(row_id, col_id)
    def edit_special_cell(self, row_id, col_id): # as original
        x, y, width, height = self.special_tree.bbox(row_id, col_id); col_index = int(col_id.replace('#', '')) - 1; current_value = self.special_tree.item(row_id, "values")[col_index]
        entry = ttk.Entry(self.special_tree); entry.place(x=x, y=y, width=width, height=height) ; entry.insert(0, current_value); entry.focus_set(); entry.selection_range(0, tk.END)
        def save_edit(): new_value = entry.get(); current_values = list(self.special_tree.item(row_id, "values")); current_values[col_index] = new_value; self.special_tree.item(row_id, values=tuple(current_values))
        entry.bind("<FocusOut>", lambda e: (save_edit(), entry.destroy())); entry.bind("<Return>", lambda e: (save_edit(), entry.destroy())); entry.bind("<Escape>", lambda e: entry.destroy())
    def save_special_mapping_tab(self): # as original
        new_map = {};
        for item_id in self.special_tree.get_children(): values = self.special_tree.item(item_id, "values"); (new_map.__setitem__(values[0].strip(), values[1].strip()) if len(values) == 2 and values[0].strip() else None)
        self.special_char_map = new_map; logging.info("Special character mapping updated."); messagebox.showinfo("Special Mapping Updated", "Special character mapping rules updated.\nSave the project to persist.")
    def create_categories_tab(self, frame): # as original
        top_frame = ttk.Frame(frame, padding=(10, 5)); top_frame.pack(fill="x"); ttk.Button(top_frame, text="Add Category", command=self.add_category).pack(side="left", padx=5); ttk.Button(top_frame, text="Remove Selected", command=self.remove_categories).pack(side="left", padx=5)
        list_frame = ttk.Frame(frame); list_frame.pack(fill="both", expand=True, padx=10, pady=(0, 10)); self.cat_listbox = tk.Listbox(list_frame, selectmode="extended", exportselection=False)
        cat_scroll = ttk.Scrollbar(list_frame, orient="vertical", command=self.cat_listbox.yview); self.cat_listbox.config(yscrollcommand=cat_scroll.set); cat_scroll.pack(side="right", fill="y"); self.cat_listbox.pack(side="left", fill="both", expand=True)
    def refresh_categories_listbox(self): # as original
        if not hasattr(self, "cat_listbox"): return
        self.cat_listbox.delete(0, tk.END);
        for cat in sorted(self.categories): self.cat_listbox.insert(tk.END, cat)
    def add_category(self): # CORRECTED VERSION
        new_cat = simpledialog.askstring("Add Category", "Enter full category path:", parent=self)
        if new_cat and new_cat.strip():
            stripped_cat = new_cat.strip()
            if stripped_cat not in self.categories:
                self.categories.append(stripped_cat)
                self.refresh_categories_listbox()
                logging.info(f"Added category: {stripped_cat}")
            else:
                logging.warning(f"Category '{stripped_cat}' already exists.")
                messagebox.showwarning("Duplicate", "Category already exists.")
        elif new_cat is not None:
            messagebox.showwarning("Invalid Input", "Category path cannot be empty.")
    def remove_categories(self): # as original
        selected_indices = self.cat_listbox.curselection()
        if not selected_indices: messagebox.showwarning("No Selection", "Please select categories to remove."); return
        selected_cats = [self.cat_listbox.get(i) for i in selected_indices]
        if messagebox.askyesno("Confirm Removal", f"Are you sure you want to remove {len(selected_cats)} selected categories?"):
            for idx in reversed(selected_indices): self.categories.pop(self.categories.index(self.cat_listbox.get(idx)))
            self.refresh_categories_listbox()
    # ... (rest of Application class methods like stop_flag_func, start_generation, _do_generation, _generation_complete, view_status_report, reprocess tab methods, project methods, on_closing, browse methods remain as original)
    def stop_flag_func(self): return self._stop_requested
    def stop_generation(self):
        if self._processing_thread and self._processing_thread.is_alive(): self._stop_requested = True; logging.warning("Stop requested. Generation will halt soon."); self.stop_button.config(state=tk.DISABLED)
        else: logging.info("Stop requested but no generation process is running.")
    def start_generation(self):
        if not self.db_available: messagebox.showerror("Database Error", "Database is not available. Cannot start generation."); return
        if self._processing_thread and self._processing_thread.is_alive(): messagebox.showwarning("Busy", "A generation process is already running."); return
        if not self.csv_file.get() or not os.path.exists(self.csv_file.get()): messagebox.showerror("Error", "Please select a valid CSV input file."); return
        if not self.xml_base.get(): messagebox.showerror("Error", "Please specify an output XML base name/path."); return
        if not self.mapping:
             if not messagebox.askyesno("No Mapping", "CSV Mapping is empty. Proceed with default mapping?"): return
        self._stop_requested = False; self.stop_button.config(state=tk.NORMAL)
        self.clear_cleansing_log()
        self._processing_thread = threading.Thread(target=self._do_generation, daemon=True); self._processing_thread.start()
    def _do_generation(self):
        logging.info("Generation thread started."); start_time = datetime.now(); updated_map = None; success = False
        try:
            try: batch_val = int(self.batch_size.get())
            except ValueError: batch_val = 7000; logging.warning("Invalid batch size, using default 7000.")
            updated_map = run_processing(csv_file=self.csv_file.get(), xml_base=self.xml_base.get(), default_location=self.default_location.get(), category=self.category.get(), username=self.username.get(), mapping=self.mapping, action=self.action.get(), node_type=self.node_type.get(), batch_size=batch_val, use_csv_createdby=self.use_csv_createdby.get(), report_file=self.report_file.get(), use_report_for_file=self.use_report_for_file.get(), csv_delimiter=self.csv_delimiter.get(), csv_quotechar=self.csv_quotechar.get(), cdata_fields=self.cdata_fields.get(), stop_flag_func=self.stop_flag_func, force_reprocess=self.force_reprocess_var.get(), cleansing_callback=self.record_cleansing_action, cleansing_options=self.get_cleansing_options())
            success = True
        except Exception as e: logging.exception("Critical error during generation process."); self.after(0, lambda: messagebox.showerror("Generation Error", f"An critical error occurred:\n{e}"))
        finally: end_time = datetime.now(); logging.info(f"Generation thread finished. Duration: {end_time - start_time}"); (setattr(self, 'mapping', updated_map) if updated_map is not None else None); self.after(0, self._generation_complete, success)
    def _generation_complete(self, success): self.stop_button.config(state=tk.DISABLED); self._stop_requested = False; self._processing_thread = None; (self.view_status_report() if success else None)
    def view_status_report(self):
        if not self.db_available: messagebox.showerror("Database Error", "Database is not available."); return
        report_lines = ["--- Processing Status Report ---"]; total_objects = 0
        try:
            status_counts = db_handler.get_status_counts(self.current_db_path); report_lines.append("\nObject Counts by Status:")
            if status_counts: [ (report_lines.append(f"- {status.capitalize()}: {count}"), globals().update({'total_objects': total_objects + count})) for status, count in sorted(status_counts.items())]; report_lines.append(f"Total Objects Tracked: {total_objects}")
            else: report_lines.append("- No objects found in database.")
            if total_objects > 0:
                type_counts = db_handler.get_file_type_counts(self.current_db_path); report_lines.append("\nSuccessful Objects by Node Type:")
                if type_counts: [report_lines.append(f"- {ntype.capitalize()}: {count}") for ntype, count in sorted(type_counts.items())]
                else: report_lines.append("- No successfully processed objects found.")
                failed_items = db_handler.get_objects_by_status(['failed'], self.current_db_path)
                if failed_items: report_lines.append(f"\nFailed Objects ({len(failed_items)}):"); [report_lines.append(f"- ID: {item['unique_id']} (Row: {item['csv_row_index']}), Identifier: {item.get('identifier', 'N/A')}, Error: {item.get('error_message', 'N/A')[:100]}") for item in failed_items[:20]]; (report_lines.append("- ... (more failures exist)") if len(failed_items) > 20 else None)
                else: report_lines.append("\nFailed Objects: None")
        except Exception as e: logging.exception("Error generating status report."); report_lines.append("\nERROR: Could not generate full report.")
        report_lines.append("--- End of Report ---"); self.log_text.insert(tk.END, "\n" + "\n".join(report_lines) + "\n"); self.log_text.see(tk.END); self.notebook.select(self.log_frame)
    def create_reprocess_tab(self, frame): # as original
        top_frame = ttk.Frame(frame, padding=(10, 5)); top_frame.pack(fill="x"); self.load_reprocess_button = ttk.Button(top_frame, text="Load _uncreated.xml File...", command=self.load_uncreated_xml_and_prepare_reprocess, state=tk.NORMAL if self.db_available else tk.DISABLED); self.load_reprocess_button.pack(side="left")
        tree_frame = ttk.Frame(frame); tree_frame.pack(fill="both", expand=True, padx=10, pady=5); columns = ("unique_id", "identifier", "error", "action"); self.reprocess_tree = ttk.Treeview(tree_frame, columns=columns, show="headings", selectmode="extended")
        self.reprocess_tree.heading("unique_id", text="Unique ID"); self.reprocess_tree.column("unique_id", width=240, anchor="w", stretch=False); self.reprocess_tree.heading("identifier", text="Identifier (Title/Loc)"); self.reprocess_tree.column("identifier", width=200, anchor="w", stretch=True); self.reprocess_tree.heading("error", text="Import Error (from XML)"); self.reprocess_tree.column("error", width=300, anchor="w", stretch=True) ; self.reprocess_tree.heading("action", text="Action"); self.reprocess_tree.column("action", width=100, anchor="center", stretch=False)
        tree_vsb = ttk.Scrollbar(tree_frame, orient="vertical", command=self.reprocess_tree.yview); tree_hsb = ttk.Scrollbar(tree_frame, orient="horizontal", command=self.reprocess_tree.xview); self.reprocess_tree.configure(yscrollcommand=tree_vsb.set, xscrollcommand=tree_hsb.set); tree_vsb.pack(side="right", fill="y"); tree_hsb.pack(side="bottom", fill="x"); self.reprocess_tree.pack(side="left", fill="both", expand=True); self.reprocess_tree.bind("<Double-1>", self.on_reprocess_tree_double_click)
        bottom_frame = ttk.Frame(frame, padding=(10, 10)); bottom_frame.pack(fill="x"); self.generate_reprocess_button = ttk.Button(bottom_frame, text="Generate Reprocess XML File...", command=self.generate_reprocess_xml, state=tk.DISABLED); self.generate_reprocess_button.pack(); self.reprocess_entries = {}
    def load_uncreated_xml_and_prepare_reprocess(self): # as original
        if not self.db_available: messagebox.showerror("Database Error", "Database is not available."); return
        xml_path = filedialog.askopenfilename(filetypes=[("Uncreated XML", "*_uncreated.xml"), ("All Files", "*.*")], title="Select Content Server _uncreated.xml File");
        if not xml_path: return
        failed_items_from_xml = []; current_error = "Unknown Error"; node_xml_lines = []; in_node = False
        try:
            with open(xml_path, 'r', encoding='utf-8') as f:
                for line in f:
                    stripped_line = line.strip(); match = re.match(r"<!-- Error:(.*)-->", stripped_line)
                    if match: current_error = match.group(1).strip()
                    elif stripped_line.startswith("<node"): node_xml_lines = [stripped_line]; in_node = True
                    elif in_node: node_xml_lines.append(stripped_line)
                    if in_node and stripped_line.endswith("</node>"):
                        in_node = False; node_xml = "\n".join(node_xml_lines); title_text = ET.fromstring(node_xml).findtext("title", default=""); loc_text = ET.fromstring(node_xml).findtext("location", default="")
                        identifier = title_text.strip() if title_text and title_text.strip() else (loc_text.strip() if loc_text and loc_text.strip() else "(Identifier unavailable)")
                        if identifier!="(Identifier unavailable)": failed_items_from_xml.append({'identifier': identifier, 'xml_error': current_error})
        except Exception as e: logging.exception(f"Error parsing _uncreated.xml: {xml_path}"); messagebox.showerror("Parse Error", f"Error reading _uncreated.xml:\n{e}"); return
        if not failed_items_from_xml: messagebox.showinfo("Info", "No failed node entries found."); self.populate_reprocess_tree([]); return
        entries_for_treeview = []; regen_errors = 0; match_errors = 0
        if not self.mapping: messagebox.showerror("Error", "Cannot regenerate: CSV Mapping empty."); return
        for failed_item in failed_items_from_xml:
            db_object = db_handler.get_object_by_identifier(failed_item['identifier'], self.current_db_path)
            if not db_object: match_errors +=1; entries_for_treeview.append({'unique_id': '(No DB Match)', 'identifier': failed_item['identifier'], 'error_message': failed_item['xml_error'], 'action_state': 'Skip'}); continue
            if not db_object.get('csv_data'): regen_errors +=1; entries_for_treeview.append({**db_object, 'error_message': 'Original CSV data missing.', 'action_state': 'Skip'}); continue
            node_elem, error_msg = process_row(db_object['csv_row_index'], db_object['csv_data'], self.mapping, self.default_location.get(), self.username.get(), self.action.get(), self.node_type.get(), self.category.get(), self.use_csv_createdby.get(), None, [], self.special_char_map, self.get_cleansing_options(), cleansing_callback=self.record_cleansing_action)
            tree_entry = {**db_object, 'identifier': failed_item['identifier'], 'error_message': failed_item['xml_error'], 'generated_xml': ET.tostring(node_elem, encoding='unicode') if node_elem else None, 'action_state': 'Re-import' if node_elem else 'Skip'}
            if not node_elem: regen_errors +=1; tree_entry['error_message'] = f"Regen Failed: {error_msg}"
            entries_for_treeview.append(tree_entry)
        self.populate_reprocess_tree(entries_for_treeview); messagebox.showinfo("Load Complete", f"Processed {len(failed_items_from_xml)} items. DB Matches: {len(failed_items_from_xml)-match_errors}. Regen Errors: {regen_errors}."); self.notebook.select(self.reprocess_frame)
    def populate_reprocess_tree(self, entries): # as original
        for item_id in self.reprocess_tree.get_children(): self.reprocess_tree.delete(item_id)
        self.reprocess_entries.clear(); self.generate_reprocess_button.config(state=tk.DISABLED)
        if not entries: return
        for entry_data in entries: iid = self.reprocess_tree.insert("", "end", values=(entry_data.get('unique_id', ''), entry_data.get('identifier', ''), entry_data.get('error_message', ''), entry_data.get('action_state', 'Skip'))); entry_data['action_tkvar'] = tk.StringVar(value=entry_data.get('action_state', 'Skip')); self.reprocess_entries[iid] = entry_data
        if entries: self.generate_reprocess_button.config(state=tk.NORMAL)
    def on_reprocess_tree_double_click(self, event): # as original
        region = self.reprocess_tree.identify("region", event.x, event.y); col_id = self.reprocess_tree.identify_column(event.x); row_id = self.reprocess_tree.identify_row(event.y)
        if region != "cell" or col_id != "#4" or not row_id or row_id not in self.reprocess_entries: return
        x,y,w,h = self.reprocess_tree.bbox(row_id, col_id); action_var = self.reprocess_entries[row_id]['action_tkvar']; can_reimport = bool(self.reprocess_entries[row_id].get('generated_xml')); available = ["Re-import", "Skip"] if can_reimport else ["Skip"]; (action_var.set("Skip") if not can_reimport and action_var.get()=="Re-import" else None)
        combo = ttk.Combobox(self.reprocess_tree, values=available, state="readonly", textvariable=action_var); combo.place(x=x,y=y,width=w,height=h); combo.focus_set()
        combo.bind("<<ComboboxSelected>>", lambda e: combo.destroy()); combo.bind("<FocusOut>", lambda e: combo.destroy()); combo.bind("<Escape>", lambda e: combo.destroy())
    def generate_reprocess_xml(self): # as original
        if not self.reprocess_entries: messagebox.showinfo("No Data", "No entries."); return
        to_export = [{'unique_id': ed.get('unique_id'), 'generated_xml': ed.get('generated_xml'), 'action_state': 'Re-import'} for ed in self.reprocess_entries.values() if ed['action_tkvar'].get() == "Re-import" and ed.get('generated_xml')]
        if not to_export: messagebox.showinfo("No Selection", "No valid entries for 'Re-import'."); return
        out_path = filedialog.asksaveasfilename(defaultextension=".xml", filetypes=[("XML Files", "*.xml")], title="Save Reprocess XML", initialfile="reprocess_import.xml")
        if not out_path: return
        try:
            ids, success = save_reprocessed_nodes(to_export, out_path)
            if success: messagebox.showinfo("Success", f"Reprocess XML saved to:\n{out_path}"); [db_handler.update_object_status(uid, 'reprocessed', db_path=self.current_db_path) for uid in ids]; self.populate_reprocess_tree([])
            else: messagebox.showwarning("File Not Saved", "Reprocess XML not saved.")
        except Exception as e: logging.exception("Error generating reprocess XML"); messagebox.showerror("Error", f"Failed: {e}")
    def open_project(self): # as original
        path = filedialog.askopenfilename(title="Open Project",filetypes=[("OI Project","*.json")]);
        if not path: return
        cfg = load_config_from_path(path);
        if not cfg: messagebox.showerror("Load Error",f"Failed to load project:\n{path}"); return
        self.current_project_path = path; self.title(f"OI Import Generator - [{os.path.basename(path)}]")
        if "categories" in cfg: self.categories = cfg["categories"]; self.refresh_categories_listbox()
        if "special_char_map" in cfg: self.special_char_map = cfg["special_char_map"]; self.populate_special_mapping_tab()
        self.load_config_values(cfg); self.populate_reprocess_tree([]); messagebox.showinfo("Project Opened", f"Loaded project:\n{path}")
    def save_project(self): # as original
        self.save_csv_mapping_tab(); self.save_special_mapping_tab() 
        path = self.current_project_path or filedialog.asksaveasfilename(title="Save Project As",defaultextension=".json",filetypes=[("OI Project","*.json")])
        if not path: return
        self.current_project_path = path; self.title(f"OI Import Generator - [{os.path.basename(path)}]")
        save_config_to_path(self.gather_current_config_dict(), path); messagebox.showinfo("Project Saved",f"Project saved to:\n{path}")
    def on_closing(self): # as original
        if self._processing_thread and self._processing_thread.is_alive(): messagebox.showwarning("Process Running", "Cannot quit while generation is in progress."); return
        if messagebox.askokcancel("Quit", "Do you really want to quit?"):
            try: save_config_to_path(self.gather_current_config_dict(), DEFAULT_CONFIG_FILE)
            except Exception as e: logging.warning(f"Could not save fallback config: {e}")
            self.destroy()
    def browse_csv(self): path = filedialog.askopenfilename(title="Select CSV",filetypes=[("CSV","*.csv")]); (self.csv_file.set(path), self.populate_csv_mapping_tab()) if path else None
    def browse_xml(self): path = filedialog.asksaveasfilename(title="Select Output XML Base",defaultextension=".xml",filetypes=[("XML","*.xml")]); self.xml_base.set(path) if path else None
    def browse_report(self): path = filedialog.askopenfilename(title="Select Report CSV",filetypes=[("CSV","*.csv")]); self.report_file.set(path) if path else None

# -------------------- Main Execution --------------------
if __name__ == "__main__":
    app = Application()
    app.mainloop()
