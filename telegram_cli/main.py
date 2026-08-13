#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
telegram_cli/main.py – Entry point for the Telegram-CLI package.

This allows running the CLI with:
    python -m telegram_cli
or after installation, just:
    tg
"""

import sys
import asyncio

# Import the main CLI function from cli.py
try:
    from telegram_cli.cli import main
except ImportError as e:
    print(f"❌ Failed to import CLI module: {e}")
    print("Make sure you are in the correct environment and the package is installed.")
    sys.exit(1)


if __name__ == "__main__":
    """
    Entry point when script is executed directly.
    Handles keyboard interrupts and other exceptions gracefully.
    """
    try:
        # Run the main CLI
        sys.exit(main())
    except KeyboardInterrupt:
        # User pressed Ctrl+C
        print("\n👋 Interrupted by user. Exiting gracefully...")
        sys.exit(0)
    except asyncio.CancelledError:
        # Async operation cancelled
        print("\n⏹️ Operation cancelled. Exiting...")
        sys.exit(0)
    except Exception as e:
        # Any other unexpected error
        print(f"\n❌ Fatal error: {e}")
        print("Please report this issue at: https://github.com/yourusername/Telegram-CLI/issues")
        sys.exit(1)