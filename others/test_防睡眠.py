import pyautogui
import time
import pyperclip
import subprocess
import sys


def restart_program():
    print('RESTART')
    python = sys.executable
    subprocess.Popen([python] + sys.argv)
    sys.exit()

time.sleep(5)

def main():
    start_time = time.time()  # Initialize the start time here

    while True:
        pyperclip.copy('正在運行爬蟲中，不要動我電腦啦幹!!')
        pyautogui.hotkey('ctrl', 'v')
        time.sleep(2)
        for _ in range(25):
            pyautogui.press('backspace')

        if time.time() - start_time >= 10:  # Check if 10 seconds have passed
            print('restart')
            restart_program()  # Call the restart function
            start_time = time.time()  # Reset the start time after restarting

if __name__ == "__main__":
    main()