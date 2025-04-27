#!/usr/bin/env python3
# Version: 2.3.18
"""
OI Import Generator with Reprocess Tab

Enhancements:
  • Adds a Reprocess tab to load _uncreated.xml files.
  • Displays error messages and associated nodes.
  • Re-generates (from the original CSV) only those rows which failed.
  • The Reprocess tab uses a scrollable Treeview with resizable columns.
  • Users can double-click the Action column to select Re-import or Skip.
  • Generates new import XMLs with selected reprocessed nodes.

Preserves all prior functionality from 2.3.17.
"""

import threading
import csv
import os
import json
import re
import xml.etree.ElementTree as ET
import tkinter as tk
from tkinter import filedialog, messagebox, ttk, simpledialog
from datetime import datetime

# -------------------- All Previous Code Stays Unchanged Here --------------------

# Default path if user never opens or saves a .json project
DEFAULT_CONFIG_FILE = os.path.join(os.path.expanduser("~"), "oi_import_config.json")

def load_config_from_path(path):
    """Reads a JSON config from 'path'. Returns {} if not found or parse error."""
    if not path or not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def save_config_to_path(config, path):
    """Writes the 'config' dictionary to the 'path' in JSON format."""
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(config, f, indent=4)
    except Exception as e:
        print("Error saving config:", e)

# A basic recognized-standard set
RECOGNISED_STANDARD = {
    "nodetype", "title", "description", "location", "created",
    "modified", "createdby", "createby", "action", "file", "category",
    "version", "docnum", "modifiedby"
}

# A MIME map for recognized file extensions
MIME_MAP = {
    "dwg": "application/x-acad",
    "arj": "application/x-arj-compressed",
    "tgz": "application/x-compressed",
    "cpio": "application/x-cpio",
    "csh": "application/x-csh",
    "dvi": "application/x-dvi",
    "emf": "application/x-emf",
    "exe": "application/x-exe",
    "gtar": "application/x-gtar",
    "gz": "application/x-gzip",
    "zip": "application/x-zip-compressed",
    "hdf": "application/x-hdf",
    "js": "application/x-javascript",
    "latex": "application/x-latex",
    "mif": "application/x-mif",
    "nc": "application/x-netcdf",
    "cdf": "application/x-netcdf",
    "msg": "application/x-outlook-msg",
    "pdf": "application/x-pdf",
    "xls": "application/x-msexcel",
    "ppt": "application/x-mspowerpoint",
    "rar": "application/x-rar-compressed",
    "sh": "application/x-sh",
    "tar": "application/x-tar",
    "tcl": "application/x-tcl",
    "tex": "application/x-tex",
    "texinfo": "application/x-texinfo",
    "tif": "image/x-tiff",
    "tiff": "image/x-tiff",
    "png": "application/x-png",
    "bmp": "application/x-bmp",
    "jpg": "image/jpeg",
    "jpeg": "image/jpeg",
    "gif": "image/gif",
    "avi": "video/x-msvideo",
    "mov": "video/x-sgi-movie",
    "flv": "video/x-flv",
    "mp3": "audio/x-mpeg",
    "wav": "audio/x-wav",
    # Office extensions
    "doc": "application/msword",
    "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation"
}

global_docnum_counter = 100000

# Our default special char map
DEFAULT_SPECIAL_CHAR_MAP = {
    "&": "and",
    "’": "'",
    "“": '"',
    "”": '"'
}

def simplify_category(full_category):
    parts = full_category.split(":")
    return parts[-1].strip() if len(parts) > 1 else full_category.strip()

def apply_special_char_replacements(text, special_map):
    for char, repl in special_map.items():
        text = text.replace(char, repl)
    return text

def wrap_cdata(text):
    return f"<![CDATA[{text}]]>"

def generate_default_mapping(header):
    """
    Creates a default column mapping based on known recognized standard columns.
    """
    mapping = {}
    for col in header:
        key = col.strip().lower()
        if key in RECOGNISED_STANDARD:
            mapping[key] = {"MappingType": "Standard", "TargetLabel": col.strip(), "Category": ""}
        else:
            mapping[key] = {"MappingType": "Metadata", "TargetLabel": col.strip(), "Category": ""}
    return mapping

def normalize_mapping(mapping):
    """
    Normalizes mapping dictionary, ensuring consistency in the structure.
    """
    norm = {}
    for key, value in mapping.items():
        norm_key = key.strip().lower()
        norm_value = {
            "MappingType": value.get("MappingType", "").strip(),
            "TargetLabel": value.get("TargetLabel", "").strip(),
            "Category": value.get("Category", "").strip()
        }
        norm[norm_key] = norm_value
    return norm

def add_standard_elements(node, std, special_map):
    """
    Creates child elements in the node for keys in a known order,
    then any 'extra' keys in alphabetical order.
    """
    primary_order = [
        "location", "title", "description", "created", "createby",
        "version", "file", "mimetype", "docnum", "createdby"
    ]
    for key in primary_order:
        if key in std:
            val = apply_special_char_replacements(std[key], special_map)
            if key.lower() == "createdby":
                ET.SubElement(node, key, attrib={"type": "0"}).text = val
            else:
                ET.SubElement(node, key).text = val

    extra_keys = sorted(k for k in std if k not in primary_order and k.lower() not in ("action", "nodetype"))
    for key in extra_keys:
        val = apply_special_char_replacements(std[key], special_map)
        ET.SubElement(node, key).text = val

def process_row(row, mapping, default_location, username, selected_action, default_node_type,
                category_default, use_csv_createdby, report_dict, rename_list, special_map):
    """
    Builds a single <node> element from a CSV row plus the UI overrides.
    Ensures MIME is always set for documents unless action=delete.
    """
    global global_docnum_counter

    std = {}
    meta_by_cat = {}

    # Normalize CSV row keys
    row = {k.strip().strip('"').lower(): v for k, v in row.items()}

    # 1) Map from CSV columns to either standard or metadata
    for col, mapinfo in mapping.items():
        value = row.get(col, "").strip()
        if mapinfo["MappingType"].lower() == "ignore":
            continue
        elif mapinfo["MappingType"].lower() == "standard":
            target = mapinfo["TargetLabel"].strip()
            if col in ("action", "nodetype"):
                target = target.lower()
            std[target] = value

            cat_str = mapinfo.get("Category", "").strip()
            if cat_str:
                for cat in [c.strip() for c in cat_str.split(",") if c.strip()]:
                    meta_by_cat.setdefault(cat, {})[target] = value

        elif mapinfo["MappingType"].lower() == "metadata":
            target = mapinfo["TargetLabel"].strip()
            if col in ("action", "nodetype"):
                target = target.lower()
            cat_str = mapinfo.get("Category", "").strip() or category_default
            cats = [c.strip() for c in cat_str.split(",") if c.strip()]
            for cat in cats:
                meta_by_cat.setdefault(cat, {})[target] = value

    # 2) Default location if not in CSV
    if default_location and "location" not in std:
        std["location"] = default_location

    # 3) Handle createdby override
    if not use_csv_createdby:
        std["createdby"] = username
    else:
        std.setdefault("createdby", username)

    # 4) If user picked an action != none, override row's action
    if selected_action.lower() != "none":
        std.setdefault("action", selected_action)

    # 5) If user picked a default_node_type != none, override row's nodetype
    if default_node_type.lower() != "none":
        std.setdefault("nodetype", default_node_type)

    # 6) Handle renaming of file/ filepath to remove colons and set MIME
    original_file = std.get("file") or std.get("filepath")
    if original_file:
        dir_name, base_name = os.path.split(original_file)
        new_base = base_name.replace(":", "")
        new_file = os.path.join(dir_name, new_base)
        if new_file != original_file:
            rename_list.append((original_file, new_file))
        if "file" in std:
            std["file"] = new_file
        elif "filepath" in std:
            std["filepath"] = new_file

        ext_field = os.path.splitext(new_base)[1].lower()
        mime_type = MIME_MAP.get(ext_field.lstrip("."), "")
        if mime_type:
            std["mimetype"] = mime_type
            if ext_field in [".eml", ".msg"]:
                std["nodetype"] = "email"
                std["action"] = "sync"

    # 7) For "update (metadata)", update the action and remove certain fields
    if std.get("action", "").lower() == "update (metadata)":
        std["action"] = "update"
        if "title" in std and "location" in std:
            if std["title"].lower() not in std["location"].lower():
                std["location"] = f"{std['location']}:{std['title']}"
        std.pop("file", None)
        std.pop("filepath", None)
        std.pop("title", None)
    else:
        ver_val = row.get("version", "").strip()
        if ver_val.isdigit() and int(ver_val) > 1:
            std["version"] = ver_val
            std["action"] = "addversion"

    # 8) Generate docnum for documents if not provided
    if std.get("nodetype", "").lower() == "document" and "docnum" not in std:
        global_docnum_counter += 1
        std["docnum"] = str(global_docnum_counter)

    # 9) Remove colons from title
    if "title" in std:
        std["title"] = std["title"].replace(":", "")

    # 10) Clean trailing colon in location
    if "location" in std and ":" in std["location"]:
        prefix_loc = std["location"].rsplit(":", 1)
        if len(prefix_loc) == 2:
            prefix, loc_tail = prefix_loc
            loc_tail = loc_tail.replace(":", "")
            std["location"] = f"{prefix}:{loc_tail}"

    # 11) For 'delete' or 'addversion', build minimal node
    action_lower = std["action"].lower()
    node_type_lower = std["nodetype"].lower()
    if action_lower in ("addversion", "delete"):
        node = ET.Element("node", attrib={"type": node_type_lower, "action": action_lower})
        loc_elem = ET.SubElement(node, "location", attrib={"type": "0"})
        loc_elem.text = std.get("location", "")
        if action_lower == "addversion":
            file_val = std.get("file") or std.get("filepath")
            if file_val:
                ET.SubElement(node, "file", attrib={"type": "0"}).text = file_val
        return node

    # 12) Ensure MIME for document nodes (if not delete)
    if node_type_lower == "document" and action_lower != "delete":
        ext = ""
        if "title" in std and std["title"]:
            _, t_ext = os.path.splitext(std["title"])
            t_ext = t_ext.lower()
            if t_ext:
                ext = t_ext
        if not ext:
            possible_file = std.get("file") or std.get("filepath","")
            if possible_file:
                base_ = os.path.basename(possible_file)
                _, f_ext = os.path.splitext(base_)
                ext = f_ext.lower()
        mime_type = ""
        if ext:
            mime_type = MIME_MAP.get(ext.lstrip("."), "")
        if not mime_type:
            mime_type = "application/octet-stream"
        std["mimetype"] = mime_type

    # 13) Build full node element
    node = ET.Element("node", attrib={"type": node_type_lower, "action": action_lower})
    add_standard_elements(node, std, special_map)

    for cat_name, meta_fields in meta_by_cat.items():
        cat_elem = ET.SubElement(node, "category", attrib={"name": cat_name})
        for k, v in meta_fields.items():
            ET.SubElement(cat_elem, "attribute", attrib={"name": k}).text = v

    return node

def serialize_element(elem, cdata_set, exclude_tags):
    """Recursively convert element->string with optional CDATA wrapping, skipping exclude_tags."""
    s = f"<{elem.tag}"
    for attr, val in elem.attrib.items():
        s += f' {attr}="{val}"'
    s += ">"
    if elem.text:
        txt = elem.text.strip()
        if txt and (("*" in cdata_set) or (elem.tag.lower() in cdata_set)):
            if not txt.startswith("<![CDATA["):
                s += wrap_cdata(txt)
            else:
                s += txt
        else:
            s += txt
    for child in elem:
        s += serialize_element(child, cdata_set, exclude_tags)
        if child.tail:
            tail = child.tail.strip()
            if tail and (("*" in cdata_set) or (child.tag.lower() in cdata_set)):
                if not tail.startswith("<![CDATA["):
                    s += wrap_cdata(tail)
                else:
                    s += tail
            else:
                s += tail
    s += f"</{elem.tag}>"
    return s

def tostring_with_cdata(elem, cdata_set):
    exclude_tags = {"import", "node"}
    s = serialize_element(elem, cdata_set, exclude_tags)
    return '<?xml version="1.0" encoding="utf-8"?>\n' + s

def write_xml_batch(nodes, output_path, cdata_fields):
    """Writes a batch of <node> elements to a single XML file with <import> as the root."""
    root = ET.Element("import")
    for node in nodes:
        root.append(node)

    if cdata_fields.strip() == "*" or cdata_fields.strip() == "*,": 
        cdata_set = {"*"}
    else:
        cdata_set = set(field.strip().lower() for field in cdata_fields.split(","))

    xml_string = tostring_with_cdata(root, cdata_set)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(xml_string)

def write_log_file(log_messages, output_dir):
    """Write the messages to a time-stamped .txt in output_dir."""
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    log_file = os.path.join(output_dir, f"import_log_{timestamp}.txt")
    with open(log_file, "w", encoding="utf-8") as lf:
        lf.write("\n".join(log_messages))
    return log_file

def generate_rename_script(rename_list, output_dir):
    """Generate a Powershell script that renames local files that had colon or other issues."""
    lines = []
    for original, new in rename_list:
        new_basename = os.path.basename(new)
        line = f'Rename-Item -Path "{original}" -NewName "{new_basename}"'
        lines.append(line)
    script_text = "\n".join(lines)
    script_path = os.path.join(output_dir, "rename_files.ps1")
    with open(script_path, "w", encoding="utf-8") as f:
        f.write(script_text)
    return script_path

def run_processing(csv_file, xml_base, default_location, category, username,
                   mapping, action, node_type, batch_size, use_csv_createdby,
                   report_file, log_text_callback, use_report_for_file,
                   csv_delimiter, csv_quotechar, cdata_fields, stop_flag_func=None):
    """
    Reads CSV, for each row calls process_row(), and writes out one or more XML batches.
    If 'stop_flag_func' is provided, we check it to see if the user requested a stop.
    """
    log_messages = []
    def log(msg):
        ts = datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")
        log_messages.append(f"{ts} {msg}")
        log_text_callback(f"{ts} {msg}\n")

    log(f"CSV file: {csv_file}")
    log(f"Output XML base: {xml_base}")
    log(f"Default location: {default_location if default_location else 'None'}")
    log(f"Default category: {category}")
    log(f"Action: {action}")
    log(f"Default node type: {node_type}")
    log(f"Batch size: {batch_size}")
    log(f"Use CSV 'createdby': {use_csv_createdby}")
    log(f"CSV Delimiter: {csv_delimiter if csv_delimiter else 'Auto-detect'}")
    log(f"CSV Quote Character: {csv_quotechar if csv_quotechar else 'Auto-detect'}")
    log(f"CDATA Fields: {cdata_fields}")

    with open(csv_file, "r", encoding="utf-8") as f:
        sample = f.read(1024)
        f.seek(0)
        if csv_delimiter:
            class CustomDialect(csv.Dialect):
                delimiter = csv_delimiter
                quotechar = csv_quotechar if csv_quotechar else '"'
                doublequote = True
                skipinitialspace = True
                lineterminator = "\r\n"
                quoting = csv.QUOTE_MINIMAL
            dialect = CustomDialect()
        else:
            try:
                dialect = csv.Sniffer().sniff(sample)
            except csv.Error:
                dialect = csv.excel

        reader = csv.DictReader(f, dialect=dialect)
        rows = list(reader)
        header = [h.lower() for h in reader.fieldnames] if reader.fieldnames else []

        if not mapping:
            mapping = generate_default_mapping(header)
        else:
            mapping = normalize_mapping(mapping)

    total_rows = len(rows)
    total_batches = (total_rows + batch_size - 1) // batch_size
    log(f"Total rows: {total_rows}, Total batches: {total_batches}")

    output_dir = os.path.dirname(os.path.abspath(xml_base))
    base_name = os.path.splitext(os.path.basename(xml_base))[0]
    ext = os.path.splitext(xml_base)[1] or ".xml"

    report_dict = {}
    if use_report_for_file and report_file and os.path.exists(report_file):
        with open(report_file, "r", encoding="utf-8") as rf:
            rep_reader = csv.DictReader(rf)
            for r in rep_reader:
                key = r["ItemName"].strip().lower()
                report_dict[key] = os.path.join(r["ParentFolder"].strip(), r["ItemName"].strip())
        log(f"Loaded {len(report_dict)} records from CSV report file.")
    else:
        report_dict = None

    rename_list = []
    batch_nodes = []
    batch_count = 0
    node_type_counts = {}

    try:
        for i, row_data in enumerate(rows):
            if stop_flag_func and stop_flag_func():
                log("Stop Generation request detected; halting early.")
                break

            node_elem = process_row(
                row_data, mapping, default_location, username, action,
                node_type, category, use_csv_createdby, report_dict,
                rename_list, DEFAULT_SPECIAL_CHAR_MAP
            )
            ntype = node_elem.attrib.get("type", "unknown")
            node_type_counts[ntype] = node_type_counts.get(ntype, 0) + 1

            batch_nodes.append(node_elem)
            if (i + 1) % batch_size == 0 or (i + 1) == total_rows:
                batch_count += 1
                batch_file = os.path.join(output_dir, f"{base_name}_{batch_count}{ext}")
                write_xml_batch(batch_nodes, batch_file, cdata_fields)
                log(f"Batch {batch_count} saved to: {batch_file}")
                batch_nodes.clear()

    except Exception as e:
        log(f"Error processing row {i+1}: {str(e)}")
        raise

    log(f"Node type counts: {node_type_counts}")

    if rename_list and not use_report_for_file:
        rename_script_path = generate_rename_script(rename_list, output_dir)
        log(f"Rename script saved to: {rename_script_path}")
    elif use_report_for_file:
        log("Using CSV report file for <file> values; no rename script generated.")

    log_file = write_log_file(log_messages, output_dir)
    log(f"Log file written to: {log_file}")

    return mapping

# -------------------- Reprocess-Related Functions --------------------
# This function remains available for backward XML-only processing if needed.
def save_reprocessed_nodes(reprocess_data, output_path):
    """Generates an XML file containing all nodes marked for re-import."""
    root = ET.Element("import")
    for item in reprocess_data:
        if item['action'].get() == 'Re-import':
            try:
                node_elem = ET.fromstring(item['node_xml'])
            except ET.ParseError:
                continue
            root.append(node_elem)

    tree_str = ET.tostring(root, encoding='utf-8')
    with open(output_path, "wb") as f:
        f.write(b'<?xml version="1.0" encoding="utf-8"?>\n')
        f.write(tree_str)

# -------------------- Tkinter Application --------------------

class Application(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("OI Import Generator (v2.3.18)")
        self.geometry("900x550")

        # Variables
        self.mapping = {}  # Will store CSV column -> {MappingType, TargetLabel, Category}
        self.csv_file = tk.StringVar()
        self.xml_base = tk.StringVar()
        self.default_location = tk.StringVar()
        self.category = tk.StringVar()
        self.username = tk.StringVar()
        self.mapping_file = tk.StringVar()
        self.report_file = tk.StringVar()
        self.action = tk.StringVar(value="sync")
        self.node_type = tk.StringVar(value="folder")
        self.batch_size = tk.IntVar(value=7000)
        self.use_csv_createdby = tk.BooleanVar(value=True)
        self.use_report_for_file = tk.BooleanVar(value=False)
        self.csv_delimiter = tk.StringVar(value="")
        self.csv_quotechar = tk.StringVar(value="")
        self.cdata_fields = tk.StringVar(value="*")

        # For categories tab
        self.categories = [
            "Content Server Categories:Pītau Categories:Pītau documents",
            "Content Server Categories:Alternate Category:Alternate Documents"
        ]
        # The special char map
        self.special_char_map = dict(DEFAULT_SPECIAL_CHAR_MAP)

        self._stop_requested = False
        self.current_project_path = None

        # Load fallback config from user home
        fallback_cfg = load_config_from_path(DEFAULT_CONFIG_FILE)
        if "special_char_map" in fallback_cfg:
            self.special_char_map = fallback_cfg["special_char_map"]
        if "categories" in fallback_cfg:
            self.categories = fallback_cfg["categories"]
        self.load_config_values(fallback_cfg)

        self.create_widgets()
        self.refresh_categories_listbox()
        self.populate_special_mapping_tab()

        self.protocol("WM_DELETE_WINDOW", self.on_closing)

    def load_config_values(self, config):
        """Populate UI from config dict if present."""
        if "csv_file" in config:
            self.csv_file.set(config["csv_file"])
        if "xml_base" in config:
            self.xml_base.set(config["xml_base"])
        if "default_location" in config:
            self.default_location.set(config["default_location"])
        if "category" in config:
            self.category.set(config["category"])
        if "username" in config:
            self.username.set(config["username"])
        if "mapping_file" in config:
            self.mapping_file.set(config["mapping_file"])
        if "report_file" in config:
            self.report_file.set(config["report_file"])
        if "action" in config:
            self.action.set(config["action"])
        if "node_type" in config:
            self.node_type.set(config["node_type"])
        if "batch_size" in config:
            self.batch_size.set(config["batch_size"])
        if "use_csv_createdby" in config:
            self.use_csv_createdby.set(config["use_csv_createdby"])
        if "use_report_for_file" in config:
            self.use_report_for_file.set(config["use_report_for_file"])
        if "csv_delimiter" in config:
            self.csv_delimiter.set(config["csv_delimiter"])
        if "csv_quotechar" in config:
            self.csv_quotechar.set(config["csv_quotechar"])
        if "cdata_fields" in config:
            self.cdata_fields.set(config["cdata_fields"])
        if "special_char_map" in config:
            self.special_char_map = config["special_char_map"]
        if "categories" in config:
            self.categories = config["categories"]
        if "csv_mapping" in config:
            self.mapping = config["csv_mapping"]

    def gather_current_config_dict(self):
        config = {}
        config["csv_file"] = self.csv_file.get()
        config["xml_base"] = self.xml_base.get()
        config["default_location"] = self.default_location.get()
        config["category"] = self.category.get()
        config["username"] = self.username.get()
        config["mapping_file"] = self.mapping_file.get()
        config["report_file"] = self.report_file.get()
        config["action"] = self.action.get()
        config["node_type"] = self.node_type.get()
        config["batch_size"] = self.batch_size.get()
        config["use_csv_createdby"] = self.use_csv_createdby.get()
        config["use_report_for_file"] = self.use_report_for_file.get()
        config["csv_delimiter"] = self.csv_delimiter.get()
        config["csv_quotechar"] = self.csv_quotechar.get()
        config["cdata_fields"] = self.cdata_fields.get()
        config["special_char_map"] = self.special_char_map
        config["categories"] = self.categories
        config["csv_mapping"] = self.mapping
        return config

    def create_widgets(self):
        self.notebook = ttk.Notebook(self)
        self.notebook.pack(fill="both", expand=True)

        self.settings_frame = tk.Frame(self.notebook)
        self.notebook.add(self.settings_frame, text="Settings")
        self.create_settings_tab(self.settings_frame)

        self.mapping_frame = tk.Frame(self.notebook)
        self.notebook.add(self.mapping_frame, text="CSV Mapping")
        self.create_csv_mapping_tab(self.mapping_frame)

        self.categories_frame = tk.Frame(self.notebook)
        self.notebook.add(self.categories_frame, text="Categories")
        self.create_categories_tab(self.categories_frame)

        self.special_frame = tk.Frame(self.notebook)
        self.notebook.add(self.special_frame, text="Special Mapping")
        self.create_special_mapping_tab(self.special_frame)

        self.log_frame = tk.Frame(self.notebook)
        self.notebook.add(self.log_frame, text="Log Output")
        self.log_text = tk.Text(self.log_frame, height=20, font=("Consolas", 12))
        self.log_text.pack(fill="both", expand=True, padx=10, pady=10)
        log_scroll = tk.Scrollbar(self.log_frame, command=self.log_text.yview)
        log_scroll.pack(side="right", fill="y")
        self.log_text.config(yscrollcommand=log_scroll.set)

        # Create Reprocess Tab (Treeview with scrolling & resizable columns)
        self.create_reprocess_tab()

    def create_settings_tab(self, frame):
        file_frame = tk.LabelFrame(frame, text="File Settings", padx=10, pady=10)
        file_frame.grid(row=0, column=0, sticky="ew", padx=10, pady=5)
        file_frame.columnconfigure(1, weight=1)

        tk.Label(file_frame, text="CSV Input File:").grid(row=0, column=0, sticky="w", pady=2)
        tk.Entry(file_frame, textvariable=self.csv_file).grid(row=0, column=1, sticky="ew", pady=2)
        tk.Button(file_frame, text="Browse", command=self.browse_csv).grid(row=0, column=2, padx=5, pady=2)

        tk.Label(file_frame, text="Output XML (base name):").grid(row=1, column=0, sticky="w", pady=2)
        tk.Entry(file_frame, textvariable=self.xml_base).grid(row=1, column=1, sticky="ew", pady=2)
        tk.Button(file_frame, text="Browse", command=self.browse_xml).grid(row=1, column=2, padx=5, pady=2)

        tk.Label(file_frame, text="CSV Report File:").grid(row=2, column=0, sticky="w", pady=2)
        tk.Entry(file_frame, textvariable=self.report_file).grid(row=2, column=1, sticky="ew", pady=2)
        tk.Button(file_frame, text="Browse", command=self.browse_report).grid(row=2, column=2, padx=5, pady=2)

        tk.Label(file_frame, text="Mapping File:").grid(row=3, column=0, sticky="w", pady=2)
        tk.Entry(file_frame, textvariable=self.mapping_file).grid(row=3, column=1, sticky="ew", pady=2)
        tk.Button(file_frame, text="Browse", command=self.browse_mapping).grid(row=3, column=2, padx=5, pady=2)

        tk.Button(file_frame, text="Load CSV Header", command=self.populate_csv_mapping_tab)\
            .grid(row=4, column=1, sticky="w", pady=5)

        import_frame = tk.LabelFrame(frame, text="Import Options", padx=10, pady=10)
        import_frame.grid(row=1, column=0, sticky="ew", padx=10, pady=5)
        import_frame.columnconfigure(1, weight=1)

        tk.Label(import_frame, text="Default Location Prefix:").grid(row=0, column=0, sticky="w", pady=2)
        tk.Entry(import_frame, textvariable=self.default_location).grid(row=0, column=1, sticky="ew", pady=2)

        tk.Label(import_frame, text="Default Category (unmapped):").grid(row=1, column=0, sticky="w", pady=2)
        tk.Entry(import_frame, textvariable=self.category).grid(row=1, column=1, sticky="ew", pady=2)

        tk.Label(import_frame, text="Created By Override:").grid(row=2, column=0, sticky="w", pady=2)
        tk.Entry(import_frame, textvariable=self.username).grid(row=2, column=1, sticky="ew", pady=2)

        tk.Label(import_frame, text="Action:").grid(row=3, column=0, sticky="w", pady=2)
        action_frame = tk.Frame(import_frame)
        action_frame.grid(row=3, column=1, sticky="w")
        for a in ["none", "sync", "addversion", "delete", "update (metadata)"]:
            tk.Radiobutton(action_frame, text=a.capitalize(), variable=self.action, value=a).pack(side="left", padx=5)

        tk.Label(import_frame, text="Default Node Type:").grid(row=4, column=0, sticky="w", pady=2)
        type_frame = tk.Frame(import_frame)
        type_frame.grid(row=4, column=1, sticky="w")
        for nt in ["none", "folder", "document"]:
            tk.Radiobutton(type_frame, text=nt.capitalize(), variable=self.node_type, value=nt)\
                .pack(side="left", padx=5)

        tk.Label(import_frame, text="Batch Size (rows per XML):").grid(row=5, column=0, sticky="w", pady=2)
        tk.Entry(import_frame, textvariable=self.batch_size).grid(row=5, column=1, sticky="w", pady=2)

        tk.Checkbutton(import_frame, text="Use CSV 'createdby'", variable=self.use_csv_createdby)\
            .grid(row=6, column=0, sticky="w", pady=2)
        tk.Checkbutton(import_frame, text="Use CSV Report for <file>", variable=self.use_report_for_file)\
            .grid(row=6, column=1, sticky="w", pady=2)

        advanced_frame = tk.LabelFrame(frame, text="Advanced CSV Options", padx=10, pady=10)
        advanced_frame.grid(row=2, column=0, sticky="ew", padx=10, pady=5)
        advanced_frame.columnconfigure(1, weight=1)

        tk.Label(advanced_frame, text="CSV Delimiter:").grid(row=0, column=0, sticky="w", pady=2)
        tk.Entry(advanced_frame, textvariable=self.csv_delimiter).grid(row=0, column=1, sticky="ew", pady=2)

        tk.Label(advanced_frame, text="CSV Quote Character:").grid(row=1, column=0, sticky="w", pady=2)
        tk.Entry(advanced_frame, textvariable=self.csv_quotechar).grid(row=1, column=1, sticky="ew", pady=2)

        tk.Label(advanced_frame, text="CDATA Fields (',' separated, '*' for all):").grid(row=2, column=0, sticky="w", pady=2)
        tk.Entry(advanced_frame, textvariable=self.cdata_fields).grid(row=2, column=1, sticky="ew", pady=2)

        btn_frame = tk.Frame(frame, padx=10, pady=10)
        btn_frame.grid(row=3, column=0, sticky="ew", padx=10, pady=5)
        tk.Button(btn_frame, text="Start Generation", command=self.start_generation, height=2, width=20)\
            .pack(side="left", padx=10)
        tk.Button(btn_frame, text="Stop Generation", command=self.stop_generation, height=2, width=20)\
            .pack(side="left", padx=10)
        tk.Button(btn_frame, text="Reprocess Import", command=self.reprocess_import, height=2, width=20)\
            .pack(side="left", padx=10)

        btn_project_frame = tk.Frame(frame, padx=10, pady=10)
        btn_project_frame.grid(row=4, column=0, sticky="ew", padx=10, pady=5)
        tk.Button(btn_project_frame, text="Open Project", command=self.open_project, height=2, width=20)\
            .pack(side="left", padx=10)
        tk.Button(btn_project_frame, text="Save Project", command=self.save_project, height=2, width=20)\
            .pack(side="left", padx=10)
        tk.Button(btn_project_frame, text="Close Project", command=self.close_project, height=2, width=20)\
            .pack(side="left", padx=10)

    def create_csv_mapping_tab(self, frame):
        top_frame = tk.Frame(frame)
        top_frame.pack(fill="x", pady=5)
        tk.Button(top_frame, text="Load CSV Header", command=self.populate_csv_mapping_tab)\
            .pack(side="left", padx=5)
        tk.Button(top_frame, text="Save CSV Mapping", command=self.save_csv_mapping_tab)\
            .pack(side="left", padx=5)

        self.csv_mapping_canvas = tk.Canvas(frame)
        self.csv_mapping_canvas.pack(side="left", fill="both", expand=True)
        self.csv_mapping_scroll = tk.Scrollbar(frame, orient="vertical", command=self.csv_mapping_canvas.yview)
        self.csv_mapping_scroll.pack(side="right", fill="y")
        self.csv_mapping_canvas.configure(yscrollcommand=self.csv_mapping_scroll.set)

        self.csv_mapping_inner = tk.Frame(self.csv_mapping_canvas)
        self.csv_mapping_canvas.create_window((0, 0), window=self.csv_mapping_inner, anchor="nw")
        self.csv_mapping_inner.bind("<Configure>",
                                    lambda e: self.csv_mapping_canvas.configure(
                                        scrollregion=self.csv_mapping_canvas.bbox("all")))

        tk.Label(self.csv_mapping_inner, text="CSV Column", borderwidth=1, relief="solid", width=20)\
            .grid(row=0, column=0, padx=5, pady=5)
        tk.Label(self.csv_mapping_inner, text="Mapping Type", borderwidth=1, relief="solid", width=15)\
            .grid(row=0, column=1, padx=5, pady=5)
        tk.Label(self.csv_mapping_inner, text="Target Label", borderwidth=1, relief="solid", width=20)\
            .grid(row=0, column=2, padx=5, pady=5)
        tk.Label(self.csv_mapping_inner, text="Category", borderwidth=1, relief="solid", width=20)\
            .grid(row=0, column=3, padx=5, pady=5)
        tk.Label(self.csv_mapping_inner, text="Select", borderwidth=1, relief="solid", width=10)\
            .grid(row=0, column=4, padx=5, pady=5)

        self.csv_mapping_entries = []

    def populate_csv_mapping_tab(self):
        for widget in self.csv_mapping_inner.winfo_children():
            if int(widget.grid_info()["row"]) > 0:
                widget.destroy()
        self.csv_mapping_entries.clear()

        csv_path = self.csv_file.get()
        if not csv_path or not os.path.exists(csv_path):
            messagebox.showerror("Error", "Please select a valid CSV file first.")
            return

        try:
            with open(csv_path, "r", encoding="utf-8") as f:
                sample = f.read(1024)
                f.seek(0)
                try:
                    dialect = csv.Sniffer().sniff(sample)
                except csv.Error:
                    dialect = csv.excel
                reader = csv.reader(f, dialect=dialect)
                headers = next(reader)
        except Exception as e:
            messagebox.showerror("Error", f"Unable to read CSV file: {str(e)}")
            return

        row_idx = 1
        for col in headers:
            norm_col = col.strip('"').strip().lower()
            default_map = self.mapping.get(norm_col, {
                "MappingType": "Standard" if norm_col in RECOGNISED_STANDARD else "Metadata",
                "TargetLabel": col.strip(),
                "Category": ""
            })

            tk.Label(self.csv_mapping_inner, text=col, borderwidth=1, relief="solid", width=20)\
                .grid(row=row_idx, column=0, padx=5, pady=5)

            cmb = ttk.Combobox(self.csv_mapping_inner, values=["Ignore", "Standard", "Metadata"], width=12)
            cmb.set(default_map.get("MappingType", "Standard" if norm_col in RECOGNISED_STANDARD else "Metadata"))
            cmb.grid(row=row_idx, column=1, padx=5, pady=5)

            ent_target = tk.Entry(self.csv_mapping_inner, width=20)
            ent_target.insert(0, default_map.get("TargetLabel", col.strip()))
            ent_target.grid(row=row_idx, column=2, padx=5, pady=5)

            cat_var = tk.StringVar()
            cat_var.set(default_map.get("Category", ""))
            cat_entry = tk.Entry(self.csv_mapping_inner, textvariable=cat_var, state="readonly", width=18)
            cat_entry.grid(row=row_idx, column=3, padx=5, pady=5)

            select_btn = tk.Button(self.csv_mapping_inner, text="Select",
                                   command=lambda v=cat_var: self.open_category_selector(v))
            select_btn.grid(row=row_idx, column=4, padx=5, pady=5)

            self.csv_mapping_entries.append((norm_col, cmb, ent_target, cat_var))
            row_idx += 1

    def save_csv_mapping_tab(self):
        new_mapping = {}
        for col, cmb, ent_target, cat_var in self.csv_mapping_entries:
            mtype = cmb.get().strip()
            target = ent_target.get().strip()
            cat = cat_var.get().strip()
            if mtype and target:
                new_mapping[col] = {"MappingType": mtype, "TargetLabel": target, "Category": cat}
        self.mapping = new_mapping
        messagebox.showinfo("Mapping Saved", "CSV mapping has been updated.")

    def open_category_selector(self, cat_var):
        win = tk.Toplevel(self)
        win.title("Select Categories")
        win.geometry("300x300")
        listbox = tk.Listbox(win, selectmode=tk.MULTIPLE)
        listbox.pack(fill="both", expand=True, padx=10, pady=10)

        full_categories = self.categories
        simple_categories = [simplify_category(c) for c in full_categories]
        for item in simple_categories:
            listbox.insert(tk.END, item)

        current_val = cat_var.get()
        if current_val:
            selected_vals = [s.strip() for s in current_val.split(",") if s.strip()]
            for idx, sc in enumerate(simple_categories):
                if sc in selected_vals:
                    listbox.selection_set(idx)

        def on_ok():
            sel = listbox.curselection()
            chosen = [full_categories[i] for i in sel]
            cat_var.set(",".join(chosen))
            win.destroy()

        tk.Button(win, text="OK", command=on_ok).pack(pady=5)

    def create_special_mapping_tab(self, frame):
        top_frame = tk.Frame(frame)
        top_frame.pack(fill="x", pady=5)
        tk.Button(top_frame, text="Save Special Mapping", command=self.save_special_mapping_tab)\
            .pack(side="left", padx=5)

        self.special_mapping_canvas = tk.Canvas(frame)
        self.special_mapping_canvas.pack(side="left", fill="both", expand=True)
        self.special_mapping_scroll = tk.Scrollbar(frame, orient="vertical", command=self.special_mapping_canvas.yview)
        self.special_mapping_scroll.pack(side="right", fill="y")
        self.special_mapping_canvas.configure(yscrollcommand=self.special_mapping_scroll.set)

        self.special_mapping_inner = tk.Frame(self.special_mapping_canvas)
        self.special_mapping_canvas.create_window((0,0), window=self.special_mapping_inner, anchor="nw")
        self.special_mapping_inner.bind("<Configure>",
            lambda e: self.special_mapping_canvas.configure(scrollregion=self.special_mapping_canvas.bbox("all")))

        tk.Label(self.special_mapping_inner, text="Special Character", borderwidth=1, relief="solid", width=20)\
            .grid(row=0, column=0, padx=5, pady=5)
        tk.Label(self.special_mapping_inner, text="Replacement", borderwidth=1, relief="solid", width=20)\
            .grid(row=0, column=1, padx=5, pady=5)

        self.special_mapping_entries = []
        self.populate_special_mapping_tab()

    def populate_special_mapping_tab(self):
        for widget in self.special_mapping_inner.winfo_children():
            if int(widget.grid_info()["row"]) > 0:
                widget.destroy()
        self.special_mapping_entries.clear()

        row_idx = 1
        for key in sorted(self.special_char_map.keys()):
            ent_key = tk.Entry(self.special_mapping_inner, width=20)
            ent_key.insert(0, key)
            ent_key.grid(row=row_idx, column=0, padx=5, pady=5)

            ent_val = tk.Entry(self.special_mapping_inner, width=20)
            ent_val.insert(0, self.special_char_map[key])
            ent_val.grid(row=row_idx, column=1, padx=5, pady=5)

            self.special_mapping_entries.append((ent_key, ent_val))
            row_idx += 1

    def save_special_mapping_tab(self):
        new_map = {}
        for ent_key, ent_val in self.special_mapping_entries:
            k = ent_key.get().strip()
            v = ent_val.get().strip()
            if k:
                new_map[k] = v
        self.special_char_map = new_map
        messagebox.showinfo("Special Mapping Saved", "Special character mapping has been updated.")

    def create_categories_tab(self, frame):
        top_frame = tk.Frame(frame)
        top_frame.pack(fill="x", padx=10, pady=5)
        tk.Button(top_frame, text="Add Category", command=self.add_category).pack(side="left", padx=5)
        tk.Button(top_frame, text="Remove Selected", command=self.remove_categories).pack(side="left", padx=5)

        self.cat_listbox = tk.Listbox(frame, selectmode="extended")
        self.cat_listbox.pack(fill="both", expand=True, padx=10, pady=5)

    def refresh_categories_listbox(self):
        if not hasattr(self, "cat_listbox"):
            return
        self.cat_listbox.delete(0, tk.END)
        for cat in self.categories:
            self.cat_listbox.insert(tk.END, cat)

    def add_category(self):
        new_cat = simpledialog.askstring("Add Category", "Enter category path:")
        if new_cat:
            self.categories.append(new_cat.strip())
            self.refresh_categories_listbox()

    def remove_categories(self):
        sel = self.cat_listbox.curselection()
        if not sel:
            return
        for idx in reversed(sel):
            self.categories.pop(idx)
        self.refresh_categories_listbox()

    def thread_safe_log(self, text):
        self.after(0, lambda: self.log_text.insert(tk.END, text))
        self.after(0, lambda: self.log_text.see(tk.END))

    def stop_flag_func(self):
        return self._stop_requested

    def stop_generation(self):
        self._stop_requested = True
        self.thread_safe_log("[Stop Requested] Generation will halt soon.\n")

    def start_generation(self):
        self._stop_requested = False
        t = threading.Thread(target=self._do_generation, daemon=True)
        t.start()

    def _do_generation(self):
        self.thread_safe_log("Starting generation...\n")
        try:
            updated_map = run_processing(
                csv_file=self.csv_file.get(),
                xml_base=self.xml_base.get(),
                default_location=self.default_location.get(),
                category=self.category.get(),
                username=self.username.get(),
                mapping=self.mapping,
                action=self.action.get(),
                node_type=self.node_type.get(),
                batch_size=self.batch_size.get(),
                use_csv_createdby=self.use_csv_createdby.get(),
                report_file=self.report_file.get(),
                log_text_callback=self.thread_safe_log,
                use_report_for_file=self.use_report_for_file.get(),
                csv_delimiter=self.csv_delimiter.get(),
                csv_quotechar=self.csv_quotechar.get(),
                cdata_fields=self.cdata_fields.get(),
                stop_flag_func=self.stop_flag_func
            )
            self.mapping = updated_map
        except Exception as e:
            self.thread_safe_log(f"ERROR: {e}\n")
        self.thread_safe_log("Generation complete.\n")

    def reprocess_import(self):
        """
        When the Reprocess Import button is selected, load a _uncreated.xml file,
        extract the failed titles, re-read the original CSV and regenerate only
        those nodes. The results are displayed in the Reprocess tab's Treeview.
        """
        xml_path = filedialog.askopenfilename(filetypes=[("Uncreated XML", "*_uncreated.xml")],
                                              title="Select _uncreated.xml")
        if not xml_path:
            return

        # Parse the _uncreated.xml file to extract failed node titles and error messages
        try:
            failed_entries = []
            current_error = None
            with open(xml_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("<!-- Object"):
                        current_error = line[5:-4].strip()
                    elif line.startswith("<node"):
                        node_lines = [line]
                        while not line.strip().endswith("</node>"):
                            line = next(f).strip()
                            node_lines.append(line)
                        node_xml = "\n".join(node_lines)
                        try:
                            node_elem = ET.fromstring(node_xml)
                            title = node_elem.findtext("title").strip() if node_elem.findtext("title") else ""
                        except ET.ParseError:
                            title = "(Invalid XML)"
                        failed_entries.append({'title': title, 'error': current_error, 'node_xml': node_xml})
                        current_error = None
        except Exception as e:
            messagebox.showerror("Parse Error", str(e))
            return

        if not failed_entries:
            messagebox.showinfo("Info", "No failed entries found in the selected XML.")
            return

        failed_titles = set(entry['title'] for entry in failed_entries if entry['title'])

        # Re-read the CSV file and regenerate only the rows with titles in failed_titles
        csv_path = self.csv_file.get()
        if not csv_path or not os.path.exists(csv_path):
            messagebox.showerror("Error", "CSV file not found. Please specify a valid CSV file.")
            return
        try:
            with open(csv_path, "r", encoding="utf-8") as f:
                sample = f.read(1024)
                f.seek(0)
                try:
                    dialect = csv.Sniffer().sniff(sample)
                except csv.Error:
                    dialect = csv.excel
                reader = csv.DictReader(f, dialect=dialect)
                rows = list(reader)
        except Exception as e:
            messagebox.showerror("Error", f"Unable to read CSV file: {e}")
            return

        reprocess_list = []
        for row in rows:
            norm_row = {k.strip().strip('"').lower(): v for k, v in row.items()}
            title_value = norm_row.get("title", "").strip()
            if title_value in failed_titles:
                node_elem = process_row(norm_row, self.mapping, self.default_location.get(),
                                         self.username.get(), self.action.get(), self.node_type.get(),
                                         self.category.get(), self.use_csv_createdby.get(),
                                         None, [], DEFAULT_SPECIAL_CHAR_MAP)
                # Look up the error message from the failed_entries using the title
                error_message = ""
                for entry in failed_entries:
                    if entry['title'] == title_value:
                        error_message = entry['error']
                        break
                # Store the regenerated node XML (as a string) and initial action ("Re-import")
                reprocess_list.append({
                    'title': title_value,
                    'error': error_message,
                    'node_xml': ET.tostring(node_elem, encoding='unicode'),
                    'action': tk.StringVar(value="Re-import")
                })

        # Populate the Treeview in the Reprocess tab with these entries
        self.populate_reprocess_tree(reprocess_list)

    def populate_reprocess_tree(self, entries):
        """
        Clear the Treeview and repopulate it with the list of entries.
        Each entry is a dict with keys: 'title', 'error', 'node_xml', and 'action' (tk.StringVar).
        """
        for item in self.reprocess_tree.get_children():
            self.reprocess_tree.delete(item)
        # Use a dictionary to store entries keyed by Treeview item ID
        self.reprocess_entries = {}
        for entry in entries:
            # Insert row with columns: Title, Error, Action
            iid = self.reprocess_tree.insert("", "end", values=(entry["title"], entry["error"], entry["action"].get()))
            self.reprocess_entries[iid] = entry

    def on_tree_double_click(self, event):
        """
        On double-click, if the click is on the 'Action' column, show a Combobox to edit its value.
        """
        region = self.reprocess_tree.identify("region", event.x, event.y)
        if region != "cell":
            return
        col = self.reprocess_tree.identify_column(event.x)
        # Assuming columns: #1=Title, #2=Error, #3=Action
        if col != "#3":
            return
        rowid = self.reprocess_tree.identify_row(event.y)
        if not rowid:
            return
        x, y, width, height = self.reprocess_tree.bbox(rowid, col)
        current_val = self.reprocess_tree.item(rowid, "values")[2]
        combobox = ttk.Combobox(self.reprocess_tree, values=["Re-import", "Skip"])
        combobox.place(x=x, y=y, width=width, height=height)
        combobox.set(current_val)
        combobox.focus_set()

        def on_select(event):
            new_val = combobox.get()
            self.reprocess_tree.set(rowid, "Action", new_val)
            if rowid in self.reprocess_entries:
                self.reprocess_entries[rowid]["action"].set(new_val)
            combobox.destroy()

        combobox.bind("<<ComboboxSelected>>", on_select)
        combobox.bind("<FocusOut>", lambda e: combobox.destroy())

    def create_reprocess_tab(self):
        """
        Creates the Reprocess tab as a Treeview with scrollbars.
        The columns are resizable.
        """
        frame = tk.Frame(self.notebook)
        self.notebook.add(frame, text="Reprocess")

        top_frame = tk.Frame(frame)
        top_frame.pack(fill="x", padx=10, pady=5)
        tk.Button(top_frame, text="Load _uncreated.xml and Regenerate Failed Rows",
                  command=self.reprocess_import).pack(side="left")

        columns = ("Title", "Error", "Action")
        self.reprocess_tree = ttk.Treeview(frame, columns=columns, show="headings")
        self.reprocess_tree.heading("Title", text="Title")
        self.reprocess_tree.heading("Error", text="Error Message")
        self.reprocess_tree.heading("Action", text="Action")
        # Set initial column widths; columns are resizable by default.
        self.reprocess_tree.column("Title", width=200, anchor="w")
        self.reprocess_tree.column("Error", width=300, anchor="w")
        self.reprocess_tree.column("Action", width=100, anchor="center")
        self.reprocess_tree.pack(fill="both", expand=True)

        vsb = ttk.Scrollbar(frame, orient="vertical", command=self.reprocess_tree.yview)
        self.reprocess_tree.configure(yscrollcommand=vsb.set)
        vsb.pack(side="right", fill="y")

        self.reprocess_tree.bind("<Double-1>", self.on_tree_double_click)

        bottom_frame = tk.Frame(frame)
        bottom_frame.pack(pady=10)
        tk.Button(bottom_frame, text="Generate Reprocess XML", command=self.generate_reprocess_xml).pack()

        self.reprocess_entries = {}  # Dictionary keyed by Treeview item ID

    def generate_reprocess_xml(self):
        """
        Generate a new XML file containing only the nodes marked with Action 'Re-import'.
        """
        if not self.reprocess_entries:
            messagebox.showinfo("No Entries", "No reprocess entries available.")
            return

        out_path = filedialog.asksaveasfilename(defaultextension=".xml",
                                                filetypes=[("XML Files", "*.xml")],
                                                title="Save Reprocess Import File")
        if not out_path:
            return

        entries_to_process = []
        for iid, entry in self.reprocess_entries.items():
            if entry["action"].get() == "Re-import":
                entries_to_process.append(entry)
        try:
            save_reprocessed_nodes(entries_to_process, out_path)
            messagebox.showinfo("Success", f"Reprocess XML saved to:\n{out_path}")
        except Exception as e:
            messagebox.showerror("Error", str(e))

    def open_project(self):
        path = filedialog.askopenfilename(
            title="Open Project",
            filetypes=[("JSON Files", "*.json"), ("All Files", "*.*")]
        )
        if not path:
            return
        loaded_cfg = load_config_from_path(path)
        if not loaded_cfg:
            messagebox.showerror("Error", f"Failed to open or parse project file:\n{path}")
            return

        self.current_project_path = path
        self.load_config_values(loaded_cfg)
        self.refresh_categories_listbox()
        self.populate_special_mapping_tab()

        messagebox.showinfo("Project Opened", f"Successfully loaded project from:\n{path}")

    def save_project(self):
        if not self.current_project_path:
            path = filedialog.asksaveasfilename(
                title="Save Project",
                defaultextension=".json",
                filetypes=[("JSON Files","*.json"), ("All Files","*.*")]
            )
            if not path:
                return
            self.current_project_path = path

        cfg = self.gather_current_config_dict()
        save_config_to_path(cfg, self.current_project_path)
        messagebox.showinfo("Project Saved", f"Project saved to:\n{self.current_project_path}")

    def close_project(self):
        if messagebox.askyesno("Close Project", "Are you sure you want to close? Unsaved data will be lost."):
            self.destroy()

    def on_closing(self):
        if messagebox.askokcancel("Quit", "Do you really want to quit?"):
            self.destroy()

    def browse_csv(self):
        path = filedialog.askopenfilename(filetypes=[("CSV Files", "*.csv"),("All Files", "*.*")])
        if path:
            self.csv_file.set(path)

    def browse_xml(self):
        path = filedialog.asksaveasfilename(defaultextension=".xml",
                                            filetypes=[("XML Files", "*.xml"), ("All Files", "*.*")])
        if path:
            self.xml_base.set(path)

    def browse_report(self):
        path = filedialog.askopenfilename(filetypes=[("CSV Files", "*.csv"), ("All Files", "*.*")])
        if path:
            self.report_file.set(path)

    def browse_mapping(self):
        path = filedialog.askopenfilename(filetypes=[("All Files", "*.*")])
        if path:
            self.mapping_file.set(path)

if __name__ == "__main__":
    app = Application()
    app.mainloop()
