import os

def combine_files(input_directory='.', output_file='combined_output.txt', file_extensions=None):
    """
    Combines contents of all files with the specified extensions in the input_directory
    into a single output_file. Each file's content is preceded by a comment with its filename.
    
    Parameters:
    - input_directory (str): Path to the directory containing files to combine.
    - output_file (str): Path to the output file.
    - file_extensions (list of str): List of file extensions to include (e.g., ['.py', '.sh']).
    """
    if file_extensions is None:
        file_extensions = ['.py', '.sh', '.yaml', '.txt', '.json']
    
    # Normalize extensions to lowercase
    file_extensions = [ext.lower() for ext in file_extensions]
    
    # Get absolute path of the input directory
    input_directory = os.path.abspath(input_directory)
    
    # Check if input directory exists
    if not os.path.isdir(input_directory):
        print(f"Error: The directory '{input_directory}' does not exist.")
        return
    
    # Open the output file in write mode
    try:
        with open(output_file, 'w', encoding='utf-8') as outfile:
            # Iterate over all files in the input directory
            for filename in sorted(os.listdir(input_directory)):
                if any(filename.lower().endswith(ext) for ext in file_extensions):
                    file_path = os.path.join(input_directory, filename)
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
    except Exception as e:
        print(f"Failed to write to output file '{output_file}': {e}")
        return

    print(f"\nAll specified files have been combined into '{output_file}'.")

if __name__ == "__main__":
    # You can modify the parameters below as needed
    combine_files(
        input_directory='.',                    # Current directory
        output_file='combined_output.txt',      # Output file name
        file_extensions=['.py', '.sh', '.yaml', '.txt', '.json']  # File extensions to include
    )
