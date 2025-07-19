import subprocess
import sys

def build():
    """
    Builds the executable using PyInstaller.
    """
    pyinstaller_command = [
        sys.executable,
        "-m",
        "PyInstaller",
        "--name",
        "OI_Import_Generator",
        "--onefile",
        "--windowed",
        "oi_import_generator.py",
    ]

    print(f"Running command: {' '.join(pyinstaller_command)}")

    try:
        subprocess.run(pyinstaller_command, check=True, capture_output=True, text=True)
        print("Build successful!")
    except subprocess.CalledProcessError as e:
        print("Build failed.")
        print(e.stdout)
        print(e.stderr)
        sys.exit(1)

if __name__ == "__main__":
    build()
