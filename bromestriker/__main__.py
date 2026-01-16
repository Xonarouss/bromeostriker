from .bot import main

if __name__ == '__main__':
    main()
from webserver import start_webserver_in_thread

start_webserver_in_thread(bot)
