import configparser

def get_db_config(section):
    config=configparser.ConfigParser()
    config.read(r'C:\Users\Aayushi\Documents\python_tutorial\question7\config.ini')
    print("Available sections: ", config.sections())
    return config[section]