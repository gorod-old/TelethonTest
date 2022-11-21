from colorama import Fore, Style


def print_exception_msg(location: str, msg: str = '', stream: int = None):
    location = f'in {location}' if location else ''
    print(Fore.MAGENTA + '[ERROR]', Fore.BLUE + f'[{stream}]', Fore.CYAN + location,
          Style.RESET_ALL + f'{msg}')


def print_info_msg(location: str = None, msg: str = '', stream: int = None):
    location = f'in {location}' if location else ''
    print(Fore.YELLOW + '[INFO]', Fore.BLUE + f'[{stream}]', Fore.CYAN + location,
          Style.RESET_ALL + f'{msg}')


def print_progress_msg(msg: str = ''):
    print(Fore.BLUE + '[PROGRESS]', Style.RESET_ALL + f'{msg}')
    print('________________________________________________________')
