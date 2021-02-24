def get_files(path):
    """ returns a list of fully qualified files for the gven path """
    # temp code to reduce total number of files we're reading
    file_list = []
    count = 0
    for item in listdir(path):
        # ! temp code to limit number of files to 1 for inital test purposes
        count = count + 1
        if count < 2:
            # ! end of temp
            if isfile(join(path, item)):
                file_list.append(join(path, item))
    logging.info("Files detected for loading: {}".format(file_list))
    return file_list
