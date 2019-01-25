import urllib2
import boto3
import botocore
from smart_open import smart_open

def get_target_filename():

    # Super important link, updated every 15 minutes
    target_url = 'http://data.gdeltproject.org/gdeltv2/lastupdate.txt'
    target_file = ''
    data = urllib2.urlopen(target_url)
    for line in data:
        target_file = line
        break

    return target_file


def get_filename(location):
    print location

    # file size, hash, link to zip
    target_link = location.split(" ")[2]
    target_file = target_link.split("/")[-1]
    target_file = target_file.replace(".zip\n", "")
    print 'Target file - ' + target_file

    return target_file


def read_s3_contents(target_file):

    twitter_topics = []

    for line in smart_open('s3://gdelt-open-data/v2/events/'+target_file, 'rb'):
        # print line.decode('utf8')
        if 'United States' not in line:
            # print line
            continue
        
        line = line.replace("\t", ";").split(';')
        # print line #[52]
        topic1 = line[6].lower()
        topic2 = line[52].split(",")[0]
        twitter_topics.append([topic1 + " " + topic2])


    print twitter_topics
    print 'Done'


if __name__ == '__main__':

    print 'Reporting for duty'

    target_file = get_target_filename()
    target_filename = get_filename(target_file)

    # read_s3_contents(target_filename.lower()) # TODO: FIX THIS
    read_s3_contents("20190124233000.export.csv")