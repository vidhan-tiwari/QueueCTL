import sys
import os

# Ensure the root workspace folder is in sys.path so we can import 'queuectl' correctly when run as a script
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from queuectl.cli import main

if __name__ == "__main__":
    main()
