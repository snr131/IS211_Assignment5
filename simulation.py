import urllib
from urllib import request
import csv
import argparse


class Queue:
    def __init__(self):
        self.items = []
    
    def is_empty(self):
        return self.items == []

    def enqueue(self, item):
        self.items.insert(0,item)

    def dequeue(self):
        return self.items.pop()

    def size(self):
        return len(self.items)


class Server:
    def __init__(self, time_request_completed):
        self.time_next_request_starts = time_request_completed
        self.current_request = None
        self.time_remaining = 0

    def busy(self):
        if self.current_request != None:
            return True
        else:
            return False

    def start_next(self, new_request):
        self.current_request = new_request


class Request:
    def __init__(self, lst):
        self.new_request_arrival_time = int(lst[0])
        self.processing_duration = int(lst[2])

    def get_stamp(self):
        return self.new_request_arrival_time

    def get_processing_duration(self):
        return self.processing_duration

    def wait_time(self, time_request_completed):
        return time_request_completed - self.new_request_arrival_time

#url = 'http://s3.amazonaws.com/cuny-is211-spring2015/requests.csv'

def simulateOneServer(myreader):

    request_queue = Queue()
    waiting_times = []

    for row in myreader:
        request = Request(row)
        request_queue.enqueue(request)
        time_request_completed = request.new_request_arrival_time
        time_next_request_starts = time_request_completed
        lone_server = Server(time_request_completed)

        if (not lone_server.busy()) and (not request_queue.is_empty()):
            if sum(waiting_times) < request.new_request_arrival_time:
                time_next_request_starts = request.new_request_arrival_time
                time_request_completed = time_next_request_starts + request.processing_duration
                next_request = request_queue.dequeue()
                waiting_times.append(next_request.wait_time(time_request_completed))
                lone_server.start_next(next_request)

            else:
                time_next_request_starts = time_request_completed
                next_request = request_queue.dequeue()
                time_request_completed = time_next_request_starts + request.processing_duration
                waiting_times.append(next_request.wait_time(time_request_completed))
                lone_server.start_next(next_request)

    average_wait = sum(waiting_times) / len(waiting_times)
    print('Average Wait %6.2f secs %3d tasks remaining.' % (average_wait, request_queue.size()))



def main(url):
    print(f"Running main with URL = {url}...")

    response = urllib.request.urlopen(url)
    file_content = response.read().decode('utf-8')
    myreader = csv.reader(file_content.splitlines())

    simulateOneServer(myreader)


if __name__ == "__main__":
    """Main entry point"""
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", help="URL to the datafile", type=str, required=False)
    args = parser.parse_args()
    main(args.url)