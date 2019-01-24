import urllib2


def read_online_txt():

    # Super important link, updated every 15 minutes
    target_url = 'http://data.gdeltproject.org/gdeltv2/lastupdate.txt'
    target_file = ''
    data = urllib2.urlopen(target_url)
    print type(data)
    for line in data:
        target_file = line
        break

    target_file = get_filename(target_file)

    print 'Done'


def get_filename(location):
    print location

    # file size, hash, link to zip
    target_link = location.split(" ")[2]
    print target_link    
    target_file = target_link.split("/")[-1]
    print target_file
    target_file = target_file.replace(".zip", "")
    print target_file

    return target_file


if __name__ == '__main__':
    print 'Reporting for duty'
    read_online_txt()