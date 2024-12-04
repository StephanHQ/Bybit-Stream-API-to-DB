import os

def combine_files(base_directory='.', additional_directory='from_cpanel_template', output_file='combined_output.txt', file_extensions=None):
    """
    Combines contents of all files with the specified extensions from:
    1. Base directory (non-recursively).
    2. Additional directory (recursively, including nested files).
    Each file's content is preceded by a comment with its filename.
    
    Parameters:
    - base_directory (str): Path to the base directory containing files to combine.
    - additional_directory (str): Path to the additional directory containing files to combine.
    - output_file (str): Path to the output file.
    - file_extensions (list of str): List of file extensions to include (e.g., ['.py', '.sh']).
    """
    if file_extensions is None:
        file_extensions = ['.py', '.sh', '.yaml', '.txt', '.json', '.log']
    
    # Normalize extensions to lowercase
    file_extensions = [ext.lower() for ext in file_extensions]

    # Helper function to combine files from a specific directory
    def process_directory(directory, outfile, recursive=False):
        directory = os.path.abspath(directory)
        if not os.path.isdir(directory):
            print(f"Error: The directory '{directory}' does not exist.")
            return
        if recursive:
            for root, _, files in os.walk(directory):
                for filename in sorted(files):
                    file_path = os.path.join(root, filename)
                    if os.path.isfile(file_path) and any(filename.lower().endswith(ext) for ext in file_extensions):
                        try:
                            with open(file_path, 'r', encoding='utf-8') as infile:
                                # Write a comment with the relative file path
                                relative_path = os.path.relpath(file_path, directory)
                                outfile.write(f"# === {relative_path} ===\n")
                                # Write the file's content
                                content = infile.read()
                                outfile.write(content + "\n\n")  # Add extra newline for separation
                            print(f"Added: {relative_path}")
                        except Exception as e:
                            print(f"Failed to read {relative_path}: {e}")
        else:
            for filename in sorted(os.listdir(directory)):
                file_path = os.path.join(directory, filename)
                if os.path.isfile(file_path) and any(filename.lower().endswith(ext) for ext in file_extensions):
                    try:
                        with open(file_path, 'r', encoding='utf-8') as infile:
                            # Write a comment with the filename
                            outfile.write(f"# === {filename} ===\n")
                            # Write the file's content
                            content = infile.read()
                            outfile.write(content + "\n\n")  # Add extra newline for separation
                        print(f"Added: {filename}")
                    except Exception as e:
                        print(f"Failed to read {filename}: {e}")

    # Open the output file in write mode
    try:
        with open(output_file, 'w', encoding='utf-8') as outfile:
            # Process files in the base directory (non-recursively)
            print(f"\nProcessing files from base directory: {base_directory}")
            process_directory(base_directory, outfile, recursive=False)

            # Process files in the additional directory (recursively)
            print(f"\nProcessing files from additional directory: {additional_directory}")
            process_directory(additional_directory, outfile, recursive=True)
    except Exception as e:
        print(f"Failed to write to output file '{output_file}': {e}")
        return

    print(f"\nAll specified files have been combined into '{output_file}'.")

if __name__ == "__main__":
    # Specify the directories and file extensions
    combine_files(
        base_directory='.',                     # Base directory (non-recursively)
        additional_directory='from_cpanel_template',  # Additional folder (recursively)
        output_file='combined_output.txt',      # Output file name
        file_extensions=['.py', '.sh', '.yaml', '.txt', '.json', '.log']  # File extensions to include
    )
