import configparser


# CREATE OBJECT
def create_config():
    config_file = configparser.ConfigParser()

    # ADD SECTION
    config_file.add_section("NetworkFactors")
    # ADD SETTINGS TO SECTION
    config_file.set("NetworkFactors", "upload", .5)
    config_file.set("NetworkFactors", "download", 1)

    # SAVE CONFIG FILE
    with open(r"config.ini", 'w') as configfileObj:
        config_file.write(configfileObj)
        configfileObj.flush()
        configfileObj.close()


