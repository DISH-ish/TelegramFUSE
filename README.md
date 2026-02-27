# Keep In Mind
Though Telegram allows file uploads, it is not intended to be used as cloud storage. Your files could be lost at any time. Don't rely on this project (or any similar ones) for storing important files on Telegram. Storing large amounts of files on this **could result in Telegram deleting your files or banning you, proceed at your own risk**.

# TelegramFS
A [FUSE](https://en.wikipedia.org/wiki/Filesystem_in_Userspace) program that stores files on Telegram.

Though I demonstrated Discord in the video too, I haven't included the code here. While I believe that storing your OWN files on Discord does NOT violate TOS, I think that spreading the code to do so might. Idk I'm trying to not actually get banned :)
Watch devdetour's video, it's cool. But i've rewritten a ton of stuff so it isn't really relevant

## Usage and How to Run
### Requirements
- Linux. Sorry Windows gamers, probably WSL would work
- Python - I used 3.14.3
- libfuse3

### To Run
Before running this, I recommend creating a virtual environment in Python.

- (optional) Create a venv with `python -m venv <your-env-name>`
- Fill out the .env file
- Run `pip install -r requirements.txt`.
    - This might fail to get pyfuse3. If it does, you may be missing some requirements. On my system (Ubuntu 20.04), running this command to install some exta packages worked for me:
    `sudo apt install meson cmake fuse3 libfuse3-dev libglib2.0-dev pkg-config`. You can also install from these directions: http://www.rath.org/pyfuse3-docs/install.html
- Enable the `user_allow_other` option in `/etc/fuse.conf`.
- Run `python main.py <path/to/your/mount>` for instance, `python3 main.py ./telegramfs` will mount at the directory `telegramfs`` in the current working dir. The directory you are mounting must exist.

## Features 

- TUI                        | It looks cool
    - python monitor.py      | Opens the TUI without starting the program, if you need the stats from the database
- Configurable blocks        | Basically the new chunk size limit
- Configurable cache         | Only size but that's better than nothing
- Stronger encryption        | For all your privacy needs
    - Anonymized data block names| Still for privacy, it's so secure you can store your data in a group of other people storing data and they won't know what it is (that's completely pointless but fun)
- --repair and --repair-only | Did you database get corrupted or deleted somehow? you can now recover it, --repair fixes the database and starts the program
- --sweep                    | Remove orphans
- --check                    | Check the database for errors
- --no-monitor               | Remove the TUI, if you wanna run this as a file server
- --debug and --debug-fuse   | --debug to debug python and --debug-fuse flushes every communication with the kernel
## .env Breakdown

- APP_ID=
- APP_HASH=                  You can get these from https://my.telegram.org/myapp. **AGAIN, storing large amounts of files could get you banned. So be careful and - take precautions if you care about losing your account.**
- CHANNEL_LINK=              the link to your Telegram channel (https://t.me/foo)
- ENCRYPTION_KEY=            64-char hex string, gets automatically generated if you choose so also you can put your own to decrypt the data
- SESSION_NAME=              this can be whatever you want, just the name that will be used for the file storing details of your Telegram session
- CACHE_MAX_BLOCKS=          optional,default: 1280=5 GiB worth of 4 MB blocks
- MAX_CONCURRENT_UPLOADS=    optional,default: 4
- MAX_CONCURRENT_DOWNLOADS=  optional,default: 4
- DELETE_BATCH_DELAY=        optional,default: 30, every xx seconds it deletes the data from the telegram that is deleted in the folder
- VERIFY_CONTENT=
- MAX_VERIFY_RETRIES=
- BLOCK_SIZE_MB=             optional,default: 10
# TO UNMOUNT, IF SOMETHING BREAKS
`fusermount -u <path/of/your/mount>`

# Features to add