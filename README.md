# Telegram-CLI

बिल्कुल भाई! ये रहा README.md का पूरा कोड – जो प्रोजेक्ट का मुख पृष्ठ (Front Page) है।

इसमें प्रोजेक्ट का पूरा विवरण, सारी फीचर्स (तुम्हारे 16 पॉइंट + Extra), Termux (32-bit) इंस्टॉलेशन गाइड, कमांड्स के उदाहरण और कॉन्फिग की जानकारी है।

---

```markdown
# 📁 Telegram CLI – Advanced Multi‑Account File Manager

> **The Ultimate Telegram File Tool** – Parallel Upload/Download, AES‑256 Encryption, Multi‑Account Rotation, SQLite Tracking, and much more, right from your terminal!

[![Python](https://img.shields.io/badge/Python-3.8%2B-blue)](https://python.org)
[![Telethon](https://img.shields.io/badge/Telethon-1.34-green)](https://docs.telethon.dev)
[![Termux](https://img.shields.io/badge/Termux-32--bit_support-brightgreen)](https://termux.com)

---

## 🚀 What is this?

**Telegram CLI** is a powerful command‑line tool that turns Telegram into your personal **cloud storage system**.  
It supports **multiple accounts**, **parallel transfers**, **file encryption**, **smart tracking**, and works perfectly even on **32‑bit Termux (Android)**.

Whether you want to:
- Upload huge files using 4‑6 parallel connections  
- Search across all your accounts at once  
- Auto‑split large files and track every piece  
- Encrypt everything before sending  
- Schedule automatic backups  

...this tool does it all with a single `tg` command.

---

## ✨ Features (What you get)

| # | Feature | Status |
|---|---------|--------|
| 1 | 🔍 **Global Search** – Search saved messages across ALL accounts | ✅ |
| 2 | ⚡ **Parallel Upload** – 4‑6 concurrent connections per file | ✅ |
| 3 | 🖥️ **RClone‑like CLI** – Full terminal interface (no Python scripts) | ✅ |
| 4 | 👥 **Multi‑Account** – Add unlimited accounts (session files stored) | ✅ |
| 5 | 🔐 **Encryption** – AES‑256‑GCM + 32‑bit password per file | ✅ |
| 6 | 📦 **Auto‑Splitting** – Large files split & rejoined automatically | ✅ |
| 7 | 🌐 **Aria2 Integration** – Download from internet (coming soon) | ⏳ |
| 8 | 📱 **32‑bit Support** – Runs perfectly on Termux (ARMv7) | ✅ |
| 9 | 📊 **Activity Log** – Complete record of all uploads/downloads | ✅ |
| 10 | 🔑 **Secure Password Store** – One master password for all files | ✅ |
| 11 | 📂 **Auto‑Splitting** – Download parts recombined seamlessly | ✅ |
| 12 | 🆔 **Unique File ID** – Every file & part gets an ID with description | ✅ |
| 13 | 🗄️ **SQLite Database** – Stores file metadata (size, hash, IP, desc) | ✅ |
| 14 | 🔄 **Account Rotation** – Automatically switches accounts to avoid bans | ✅ |
| 15 | 😴 **Random Breaks/Sleeps** – Human‑like delays to stay undetected | ✅ |
| 16 | 🔌 **Plugin Architecture** – Add to other projects for data recovery | ✅ |

### 🌟 Extra Goodies (Added as per your request)
- 📦 **Batch Processing with Queue** – Upload/download multiple files in a queue  
- ✅ **File Integrity Check** – MD5/SHA256 checksum verification  
- ⏰ **Scheduled Jobs** – Cron‑like automation (upload every night)  
- 📤 **Export/Import Config** – Move your setup to another machine instantly  

---

## 📥 Installation

### For **Termux (Android – 32‑bit/64‑bit)**

```bash
# 1. Update packages and install Python
pkg update && pkg upgrade -y
pkg install python -y
pkg install binutils -y  # required for some dependencies

# 2. Clone the repository
git clone https://github.com/yourusername/telegram-cli.git
cd telegram-cli

# 3. Create a virtual environment (optional but recommended)
python -m venv venv
source venv/bin/activate

# 4. Install dependencies
pip install --upgrade pip
pip install -r requirements.txt

# 5. Install the CLI tool itself
pip install -e .

# 6. Verify installation
tg --version
```

For Linux / macOS / Windows (WSL)

```bash
# Install Python 3.8+ if not already installed
sudo apt install python3 python3-pip git  # Debian/Ubuntu

git clone https://github.com/yourusername/telegram-cli.git
cd telegram-cli
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
pip install -e .
tg --help
```

---

⚙️ Configuration (First Time Setup)

1. Get API credentials from my.telegram.org

2. Set environment variables (optional but recommended)

```bash
cp .env.example .env
nano .env
```

Add your API_ID and API_HASH.

3. Add your first Telegram account

```bash
tg accounts add --api-id YOUR_API_ID --api-hash YOUR_API_HASH --phone +91XXXXXXXXXX
```

You will be prompted to enter the OTP sent to your Telegram.

4. Set a master password (for encryption)

```bash
tg config set master-password
```

This password will be used to encrypt the password_store.enc file.

---

📖 Command Reference

🧑‍🤝‍🧑 Account Management

```bash
# Add a new account
tg accounts add --api-id 12345 --api-hash abcdef --phone +91XXXXXXXXXX

# List all configured accounts
tg accounts list

# Remove an account
tg accounts remove +91XXXXXXXXXX
```

⬆️ Upload Files

```bash
# Upload a file with default settings (asks for description)
tg upload /path/to/file.mp4

# Upload with a specific description and channel
tg upload /path/to/file.mp4 --description "My Movie" --channel @myprivatechannel

# Upload using a specific account (by phone)
tg upload /path/to/file.mp4 --accounts +91XXXXXXXXXX

# Upload a folder (batch)
tg upload /path/to/folder --batch --parallel 4
```

⬇️ Download Files

```bash
# Download by file ID (you get this from the database or search)
tg download FILE_ID_12345

# Download to a specific location
tg download FILE_ID_12345 --output /sdcard/Download/

# Download all files with a specific description
tg download --description "My Movie" --all
```

🔍 Search

```bash
# Search for "xyz" in all accounts
tg search xyz

# Search only in a specific channel
tg search xyz --channel @myprivatechannel
```

📊 Statistics

```bash
# Show total uploaded/downloaded data per account
tg stats

# Show detailed file list
tg stats --detailed
```

⏰ Scheduled Jobs

```bash
# List all scheduled jobs
tg scheduler list

# Add a daily backup job at 2 AM
tg scheduler add --cron "0 2 * * *" --command "upload /data/backup/ --channel @backup"

# Remove a job
tg scheduler remove JOB_ID
```

🔧 Configuration & Export

```bash
# Show current config
tg config show

# Export all config (accounts, settings) to a file
tg config export --output telegram_config.tar.gz

# Import config from file
tg config import telegram_config.tar.gz
```

---

🗄️ Database & File Tracking

Every file you upload/download is recorded in a SQLite database (data/database/telegram_cli.db).
It stores:

· File ID (unique per file)
· File name, size, MD5/SHA256 hash
· Description (user‑provided, unique per file)
· Upload/Download date & time
· Account & Channel used
· User IP (local IP)
· Parts list (if split)
· Encryption status

You can query the database directly:

```bash
sqlite3 data/database/telegram_cli.db "SELECT * FROM file_records WHERE description LIKE '%movie%';"
```

---

🧩 Plugin System (for other projects)

You can use this tool as a data recovery plugin in other projects.
Simply import the core modules:

```python
from telegram_cli.core.downloader import Downloader
from telegram_cli.core.uploader import Uploader
from telegram_cli.database.db_manager import DatabaseManager

# Use functions directly in your own code
db = DatabaseManager()
uploader = Uploader(client_pool)
uploader.upload_file("/path/to/file.mp4", description="Recovery backup")
```

Check the plugins/ folder for examples.

---

🛠️ Development & Testing

Run tests

```bash
pytest tests/
```

Code style

```bash
# Install dev dependencies
pip install black flake8

# Format code
black telegram_cli/

# Check linting
flake8 telegram_cli/
```

---

📂 Directory Structure (Overview)

```
telegram-cli/
├── data/                 # User data (sessions, config, DB, logs)
├── telegram_cli/         # Main source code
│   ├── core/            # All logic (upload, download, encrypt, etc.)
│   ├── models/          # Data schemas
│   ├── database/        # SQLite ORM
│   ├── utils/           # Helpers (logger, config, password)
│   └── plugins/         # Plugin architecture
├── tests/               # Unit tests
├── docs/                # Full documentation
├── requirements.txt
├── setup.py
└── README.md            # You are here!
```

---

❓ FAQ

Q: Does this work on 32‑bit Termux?
✅ Yes! All dependencies are pure Python and compile on ARMv7.

Q: What is the maximum file size?
Telegram allows 2GB (free) and 4GB (premium). This tool auto‑splits larger files into parts.

Q: How does account rotation work?
The client_pool rotates accounts in a round‑robin manner. You can also set random sleeps between requests.

Q: Can I use the same channel for all accounts?
Yes. If all accounts use the same private channel, the script still rotates accounts but sends all files to that one channel.

Q: Where are my passwords stored?
They are encrypted in data/passwords/password_store.enc using your master password.

Q: How do I recover data if I lose the master password?
❗ Important: You cannot recover encrypted files without the master password. Keep it safe!

---

📝 License

This project is licensed under the MIT License – feel free to use, modify, and distribute.

---

🤝 Contributing

Pull requests are welcome! For major changes, please open an issue first to discuss what you would like to change.

1. Fork the project
2. Create your feature branch (git checkout -b feature/AmazingFeature)
3. Commit your changes (git commit -m 'Add some AmazingFeature')
4. Push to the branch (git push origin feature/AmazingFeature)
5. Open a Pull Request

---

🙏 Acknowledgements

· Telethon – Core Telegram MTProto library
· Click – CLI interface
· Cryptography – AES encryption
· SQLAlchemy – ORM for SQLite

---

Made with ❤️ for the Telegram community.
