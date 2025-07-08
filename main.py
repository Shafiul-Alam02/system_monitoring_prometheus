import subprocess







def main():
    # Updated list of your files in desired order
    scripts = ['write_into_db.py', 'sheet_to_looker.py', 'daily.py', 'monthly.py']

    for script in scripts:
        print(f"\n Running {script}...")
        try:
            subprocess.run(['python', script], check=True)
            print(f"Successfully ran {script}")
        except subprocess.CalledProcessError as e:
            print(f"Error while running {script}: {e}")
            break  # Stop if any script fails



if __name__ == "__main__":
    main()
