# OI Import Generator

The OI Import Generator is a Python-based desktop application designed to facilitate the conversion of CSV (Comma Separated Values) data into XML (Extensible Markup Language) format. This XML output is structured for import into systems like OpenText Content Server (often referred to as "OI"). The tool provides a graphical user interface (GUI) for easy configuration of mappings, settings, and for managing the generation process. It includes features for tracking item processing status via a local SQLite database and for reprocessing items that may have failed during a previous import attempt.

## Key Features

*   **CSV to XML Conversion:** Converts data from CSV files into a structured XML format suitable for import into target systems.
*   **Graphical User Interface (GUI):** Provides an intuitive Tkinter-based interface for managing all aspects of the conversion process.
*   **Database Integration:** Utilizes an SQLite database (`oi_processing_status.db`) to track the status (pending, processing, success, failed, reprocessed) of each item from the CSV.
*   **Column Mapping:** Allows users to map CSV columns to specific XML elements, including standard fields (e.g., title, location) and custom metadata attributes.
*   **Category Management:** Supports defining and assigning items to multiple categories within the target system.
*   **Special Character Handling:** Provides a configurable mapping for replacing special characters to ensure XML validity and data cleanliness.
*   **Batch Processing:** Generates XML output in batches of configurable size, improving manageability for large datasets.
*   **Reprocess Functionality:** Enables users to load an XML file of previously failed items (e.g., `_uncreated.xml` from Content Server) and attempt to regenerate the XML for them using the original CSV data stored in the database.
*   **Configuration Management:** Allows users to save all settings (file paths, mappings, categories, etc.) into a JSON project file and load them for later use. A default configuration (`oi_import_config.json`) is also used for fallback.
*   **Logging:** Comprehensive logging of operations, errors, and progress, displayed within the GUI and saved to a log file (`oi_generator.log`).
*   **File Renaming Script:** Can generate a PowerShell script to rename files that contain characters problematic for file systems or URLs (e.g., colons).
*   **Customizable Overrides:** Offers options to override 'action' (sync, addversion, delete), 'node type' (folder, document), and 'createdby' fields.

## Usage

The OI Import Generator is designed to be user-friendly. Here's a general workflow:

1.  **Launch the Application:**
    *   Ensure you have Python 3 installed.
    *   Run the `OI Import Generator.py` script: `python "OI Import Generator.py"`
    *   The main application window will appear with several tabs.

2.  **Project Setup (Settings Tab):**
    *   **CSV Input File:** Click "Browse..." to select your source CSV file. Once loaded, you can click "Load CSV Header -> Mapping Tab" to populate the CSV Mapping tab.
    *   **Output XML (base name):** Click "Browse..." to specify the base name and location for your output XML files (e.g., `C:\output\import_batch`). The tool will append batch numbers and `.xml` (e.g., `import_batch_1.xml`).
    *   **Default Location Prefix:** Enter a default location prefix if your CSV data for locations is relative (e.g., `Enterprise:StagingArea:`). This will be prepended to location values from the CSV if the "location" field is mapped.
    *   **Default Category (if unmapped):** Specify a default category to be used if a metadata item's category is not explicitly defined in the mapping tab.

3.  **Migration Type (Settings Tab):**
    *   **Action Override:** Choose a global action for all items:
        *   `none`: Relies on 'action' being mapped from the CSV.
        *   `sync`: Standard import/update action.
        *   `addversion`: To add new versions to existing items.
        *   `delete`: To mark items for deletion.
        *   `update (metadata)`: To update only metadata for existing items (file information will be excluded).
    *   **Default Node Type Override:** Choose a global node type:
        *   `none`: Relies on 'nodetype' being mapped from the CSV.
        *   `folder`: All items will be treated as folders.
        *   `document`: All items will be treated as documents.

4.  **Advanced & Optional Settings (Settings Tab):**
    *   **CSV Report File (Optional):** Specify a CSV report file if needed for external tracking.
    *   **Created By Override:** Set a default username for the 'createdby' field. This can be overridden by a mapped CSV column if "Use 'createdby' column from CSV" is checked.
    *   **Batch Size:** Define the number of records per output XML file.
    *   **Checkboxes:**
        *   `Use 'createdby' column from CSV`: If your CSV has a column for 'createdby', check this to use its values.
        *   `Use CSV Report File for <file> path`: If file paths in the XML should refer to a report file.
        *   `Force Reprocess Successful Items`: If checked, the main generation process will re-process items even if they are marked as 'success' in the database.
    *   **Advanced CSV Parsing:**
        *   `CSV Delimiter`: Specify the delimiter if your CSV doesn't use a comma (e.g., `;`, `\t`). Leave blank for auto-detection.
        *   `CSV Quote Char`: Specify the quote character if it's not a double quote. Leave blank for auto-detection.
        *   `CDATA Fields`: List CSV column *target labels* (comma-separated) whose text content should be wrapped in CDATA sections in the XML. Use `*` to wrap all text content in CDATA.

5.  **Configure CSV Column Mappings (CSV Mapping Tab):**
    *   After loading a CSV in the Settings tab, click "Load CSV Header" on this tab (or the button on the Settings tab).
    *   For each CSV column header listed:
        *   **Mapping Type:**
            *   `Ignore`: This column will not be included in the XML.
            *   `Standard`: Maps to a predefined XML element (e.g., title, location, description, file, version, createdby).
            *   `Metadata`: Maps to a custom metadata attribute within a category.
        *   **Target Label:** The name of the XML element or attribute. For "Standard" types, this should match recognized fields (e.g., `title`, `location`).
        *   **Category:** For "Metadata" type, specify the Content Server category name (e.g., `Enterprise:General`). You can assign multiple categories by separating them with a comma (e.g., `Cat1,Folder:Cat2`). Click the "..." button to select from a list of predefined categories (managed in the "Categories" tab).
    *   Click **"Save Column Mappings"** to apply your changes. *This is important!*

6.  **Manage Categories (Categories Tab):**
    *   Add or remove category paths that will be available in the "CSV Mapping" tab's category selector. This helps maintain consistency.

7.  **Manage Special Character Mappings (Special Mapping Tab):**
    *   Review and modify the default mappings for special characters (e.g., `&` to `and`).
    *   You can add new rows or remove existing ones.
    *   Click **"Apply & Save Special Mapping"** to save your changes for the current session.

8.  **Start Generation (Settings Tab):**
    *   Once all settings and mappings are configured, click **"Start Generation"**.
    *   Progress will be displayed in the "Log Output" tab.
    *   A "Stop Generation" button will become active to halt the process if needed.
    *   Upon completion or if stopped, a status report can be viewed by clicking "View Status Report".

9.  **Using the Reprocess Tab:**
    *   If you have an `_uncreated.xml` file (or similar) from a previous import attempt by Content Server, click **"Load _uncreated.xml File..."**.
    *   The tool will parse this file, identify failed items, and attempt to find their original data in the `oi_processing_status.db` database using the item's identifier (title or location).
    *   It will then try to regenerate the XML for these items using the stored CSV data and current mappings.
    *   The list of items will appear in the table:
        *   **Unique ID:** The internal ID from the database.
        *   **Identifier (Title/Loc):** The title or location of the item.
        *   **Import Error (from XML):** The error message reported by Content Server.
        *   **Action:** Defaults to `Re-import` if XML could be regenerated, `Skip` otherwise. You can change this by double-clicking the "Action" cell.
    *   Once reviewed, click **"Generate Reprocess XML File..."** to save a new XML file containing only the items marked for "Re-import".
    *   Successfully reprocessed items will have their status updated to 'reprocessed' in the database.

10. **Saving and Loading Projects:**
    *   **Save Project...:** (Settings Tab) Saves the current configuration (file paths, mappings, categories, special characters) to a JSON file (e.g., `my_project.json`). This allows you to easily reload your settings later.
    *   **Open Project...:** (Settings Tab) Loads a previously saved JSON project file, restoring all its settings.

11. **Log Output Tab:**
    *   Displays real-time logging messages during the generation process.
    *   Also shows status reports.
    *   Logs are saved to `oi_generator.log` in the application's directory.

## Database Schema

The application uses an SQLite database named `oi_processing_status.db` (located in the same directory as the application) to keep track of the processing status of each object (row) from your input CSV file. This database allows the application to:

*   Avoid reprocessing items that were already successfully processed (unless forced).
*   Identify and retrieve data for items that failed, for use in the "Reprocess" tab.
*   Provide reports on processing status.

The main table in the database is `objects`. Here are its key columns:

*   **`unique_id` (TEXT, PRIMARY KEY):** A unique identifier (UUID) generated for each CSV row upon its first ingestion. This ID is used for internal tracking.
*   **`csv_row_index` (INTEGER):** The original row number from the input CSV file (1-based).
*   **`status` (TEXT):** The current processing status of the object. Common values include:
    *   `pending`: The object has been read from the CSV and is awaiting processing.
    *   `processing`: The object is currently being processed.
    *   `success`: The object was successfully processed and its XML was generated.
    *   `failed`: An error occurred while processing the object.
    *   `reprocessed`: The object was successfully processed via the "Reprocess" tab.
*   **`node_type` (TEXT):** The determined node type for the object (e.g., `folder`, `document`).
*   **`action` (TEXT):** The import action determined for the object (e.g., `sync`, `addversion`, `delete`).
*   **`identifier` (TEXT):** A display identifier for the object, typically the `title` or `location` field, used for matching in the reprocess functionality.
*   **`generated_xml` (TEXT):** The generated XML snippet for the object if processing was successful.
*   **`error_message` (TEXT):** Any error message recorded if the object processing failed.
*   **`output_batch_file` (TEXT):** The name of the XML batch file to which this object's XML was written.
*   **`last_attempt_timestamp` (TIMESTAMP):** The date and time of the last processing attempt for this object.
*   **`csv_data_json` (TEXT):** The original CSV row data, stored as a JSON string. This is crucial for the "Reprocess" functionality, as it allows the tool to use the original data to regenerate XML.

## Configuration Files

The OI Import Generator uses JSON files to store configurations. This allows you to save and reload your settings for different import projects.

1.  **Project Configuration Files (`*.json`):**
    *   When you use the "Save Project..." feature (Settings Tab), all current settings are saved into a JSON file you name (e.g., `my_migration_project.json`).
    *   This file includes:
        *   File paths (CSV input, XML output base, report file).
        *   Default settings (location, category, username).
        *   Migration type settings (action, node type).
        *   Advanced settings (batch size, boolean flags, CSV parsing options, CDATA fields).
        *   The complete CSV column mapping (`csv_mapping`).
        *   The list of managed categories (`categories`).
        *   The special character replacement map (`special_char_map`).
    *   You can load these project files using "Open Project..." to quickly restore a specific configuration.

2.  **Default Fallback Configuration (`oi_import_config.json`):**
    *   A file named `oi_import_config.json` is automatically created and updated in your user's home directory (e.g., `C:\Users\YourUser\oi_import_config.json`).
    *   This file stores the last used settings when you close the application.
    *   If you open the application without loading a specific project file, it will attempt to load settings from this default fallback configuration. This provides a convenient way to resume with your most recent general settings.
    *   The structure of this file is the same as the project-specific configuration files.

**Main Configuration Sections (within the JSON):**

*   `csv_file`, `xml_base`, `default_location`, `category`, `username`, `mapping_file` (legacy, path to separate mapping if used), `report_file`: Store respective path and string settings.
*   `action`, `node_type`: Store the selected migration overrides.
*   `batch_size`: Stores the integer value for batch processing.
*   `use_csv_createdby`, `use_report_for_file`, `force_reprocess_var` (internal name for "Force Reprocess Successful Items"): Boolean flags.
*   `csv_delimiter`, `csv_quotechar`, `cdata_fields`: Advanced CSV parsing settings.
*   `special_char_map`: An object where keys are special characters and values are their replacements.
    ```json
    "special_char_map": {
        "&": "and",
        "’": "'",
        "“": "\"",
        "”": "\""
    }
    ```
*   `categories`: A list of category strings.
    ```json
    "categories": [
        "Content Server Categories:Pītau Categories:Pītau documents",
        "Content Server Categories:Alternate Category:Alternate Documents"
    ]
    ```
*   `csv_mapping`: An object where keys are normalized CSV column headers and values are objects defining their mapping.
    ```json
    "csv_mapping": {
        "csv_column_name_lowercase": {
            "MappingType": "Standard" / "Metadata" / "Ignore",
            "TargetLabel": "XMLTargetName",
            "Category": "OptionalCategoryName"
        }
    }
    ```

Understanding these configuration files can be helpful for advanced users who might want to review or (carefully) manually adjust settings outside the application.

## Dependencies and Setup

**1. Python:**
*   This application is written in Python 3. You will need a Python 3 interpreter installed on your system. It has been developed and tested with Python 3.7+ but should generally work with most recent Python 3 versions.

**2. Standard Libraries:**
*   The application primarily uses Python's standard libraries, which are included with your Python installation. These include:
    *   `tkinter` (for the GUI)
    *   `sqlite3` (for database interaction)
    *   `csv`
    *   `json`
    *   `xml.etree.ElementTree`
    *   `os`
    *   `logging`
    *   `datetime`
    *   `re`
    *   `threading`
    *   `queue`
    *   `uuid`
*   No external package installations (e.g., via `pip`) are typically required if you have a standard Python 3 environment.

**3. Local Files:**
*   **`db_handler.py`:** This file, which contains the database interaction logic, must be present in the same directory as the main application script (`OI Import Generator.py`).
*   **`oi_processing_status.db`:** This SQLite database file will be automatically created in the same directory as the application when it first runs or needs to log/track data.
*   **`oi_generator.log`:** A log file that will be automatically created in the same directory, recording operational messages and errors.

**Setup Steps:**

1.  Ensure Python 3 is installed on your system.
2.  Place the `OI Import Generator.py` script and the `db_handler.py` script in the same directory.
3.  Run the application using:
    ```bash
    python "OI Import Generator.py"
    ```
    (On some systems, you might use `python3` instead of `python`).
4.  The application should start, and the GUI will appear. The database (`oi_processing_status.db`) and log file (`oi_generator.log`) will be created automatically in that directory if they don't already exist.

## Running Tests

The project includes a suite of unit tests in the `test_processing.py` file. These tests cover functionalities in both `db_handler.py` and core components of `OI Import Generator.py`.

To run the tests:

1.  **Navigate to the Directory:**
    Open your terminal or command prompt and navigate to the directory where `test_processing.py`, `OI Import Generator.py`, and `db_handler.py` are located.

2.  **Execute the Test Script:**
    You can run the tests using Python's built-in `unittest` module.

    ```bash
    python -m unittest test_processing.py
    ```
    Or, if `test_processing.py` is made executable and has the appropriate shebang:
    ```bash
    ./test_processing.py
    ```

3.  **Interpret Results:**
    The tests will run, and you'll see output indicating the status of each test (e.g., `.` for pass, `F` for failure, `E` for error). A summary at the end will show the total number of tests run and any failures or errors.

It's good practice to run these tests if you make any modifications to the codebase to ensure that existing functionality remains intact.

## Contributing

Contributions to the OI Import Generator are welcome! If you have suggestions for improvements, bug fixes, or new features, please consider the following:

1.  **Open an Issue:** For significant changes or bug reports, it's a good idea to open an issue first to discuss the problem or proposed feature. This allows for discussion before development work begins.
2.  **Fork the Repository:** If you plan to contribute code, fork the repository on GitHub (if applicable, assuming it's hosted there).
3.  **Create a Branch:** Create a new branch in your fork for your changes.
4.  **Make Changes:** Implement your bug fix or feature.
5.  **Test Your Changes:** Ensure your changes don't break existing functionality. Running the unit tests in `test_processing.py` is highly recommended. Consider adding new tests for new features.
6.  **Write Clear Commit Messages:** Follow standard practices for writing clear and informative commit messages.
7.  **Submit a Pull Request:** Once your changes are ready, submit a pull request from your branch to the main repository. Provide a clear description of the changes in the pull request.

Even if you don't contribute code, feedback and bug reports are also valuable.

## License

This project is currently not under a specific license. It is recommended to add an open-source license if you intend for others to use, modify, or distribute the code.

If you choose to use the MIT License, a common and permissive open-source license, you can add the following text. Remember to replace `[Year]` and `[Full Name]` with the appropriate information.

```
MIT License

Copyright (c) [Year] [Full Name]

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

You should also create a `LICENSE` file in the root of your project containing the full license text.
