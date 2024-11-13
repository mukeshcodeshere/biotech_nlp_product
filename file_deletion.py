import os
import shutil
import psutil
import streamlit as st
from concurrent.futures import ThreadPoolExecutor, as_completed

# Function to find processes using files and kill them
def kill_processes_using_files(file_paths):
    processes_to_terminate = set()
    
    # Iterate over all processes and find those using any of the files
    for proc in psutil.process_iter(['pid', 'name', 'open_files']):
        try:
            for file in proc.info['open_files'] or []:
                if file.path in file_paths:
                    processes_to_terminate.add(proc)
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            continue

    # Now terminate all processes
    for proc in processes_to_terminate:
        st.warning(f"Process {proc.info['name']} (PID: {proc.info['pid']}) is using a file. Terminating process.")
        proc.terminate()  # Forcefully terminate the process
        proc.wait()  # Wait for the process to terminate
        st.info(f"Process {proc.info['name']} terminated.")

# Function to delete a single file
def delete_file(file_path):
    try:
        # Force delete the file
        os.remove(file_path)
        st.info(f"Deleted file {file_path}")
    except PermissionError as e:
        st.error(f"PermissionError: {e} while deleting file {file_path}")
    except Exception as e:
        st.error(f"Error deleting file {file_path}: {str(e)}")

# Function to delete a single directory
def delete_directory(dir_path):
    try:
        # Force delete the directory and all its contents
        shutil.rmtree(dir_path)
        st.info(f"Deleted directory {dir_path}")
    except PermissionError as e:
        st.error(f"PermissionError: {e} while deleting directory {dir_path}")
    except Exception as e:
        st.error(f"Error deleting directory {dir_path}: {str(e)}")

# Function to delete all files and folders in a directory forcefully
def delete_existing_files(base_dir):
    if os.path.exists(base_dir):
        # Collect all file paths and directories in base_dir
        file_paths = []
        dirs_to_delete = []
        for root, dirs, files in os.walk(base_dir, topdown=False):
            for name in files:
                file_paths.append(os.path.join(root, name))
            for name in dirs:
                dirs_to_delete.append(os.path.join(root, name))

        # Step 1: Kill processes using any of the files
        kill_processes_using_files(file_paths)
        
        # Step 2: Delete files using threading for faster execution
        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(delete_file, file_path) for file_path in file_paths]
            for future in as_completed(futures):
                pass  # Wait for all file deletions to complete
        
        # Step 3: Delete directories (in parallel for faster execution)
        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(delete_directory, dir_path) for dir_path in dirs_to_delete]
            for future in as_completed(futures):
                pass  # Wait for all directory deletions to complete

        st.info(f"All files and directories in {base_dir} have been deleted.")
    else:
        st.warning(f"Directory {base_dir} does not exist.")
