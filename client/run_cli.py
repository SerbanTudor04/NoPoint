"""
run_cli.py
===========
Entry point for the NoPoint Drive interactive CLI.

    python run_cli.py
    python run_cli.py --host 10.0.0.1 --user alice
    python run_cli.py --sync ~/Documents/NoPoint --save
"""

from cli.main import run

if __name__ == "__main__":
    run()