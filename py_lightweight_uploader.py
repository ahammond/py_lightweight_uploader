#!/usr/bin/env python

from httplib import HTTPConnection
from logging import debug, info, warning, critical
from mimetypes import guess_type
from os.path import getsize
from random import randint
import re
from threading import Thread, Lock
from time import sleep
from urllib import quote_plus
from urlparse import urlparse, ParseResult, urlunparse
from uuid import uuid4

__author__ = 'Andrew Hammond <andrew.hammond@receipt.com>'
__copyright__ = 'Copyright (c) 2011 SmartReceipt'
#TODO: what license? Should match ngnix, I guess.
__license__ = 'GNU GPL v2???'
__vcs_id__ = '$Id$'


# Note: this pattern recognizes the first contiguous segment of the file.
# There may be other segments already uploaded.
# In that case, we ignore the subsequent segments and simply find the first
# MISSING segment. This doesn't necessarily mean we will re-upload anything,
# since, after uploading the first missing segment, we check again.
RECEIVED_RANGE_PATTERN = re.compile(r'^0-(?P<last_byte_received>\d+)')

class UploadQueueEntry(object):
    def __init__(self, id, file):
        self.id = id
        self.file = file

class LightweightUploader(Thread):
    """
    A minimal implementation of ngnix compatible resumable upload.
    API is based on that of Lightweight Uploader http://lwu.no-ip.org/

    Does not support
    - User interface (this is a back-end only module)
    - Concurrent upload of chunks. Our goal here is reliable, resumable uploads, not performance.
      That said, the author has no objections to extending this to be more performant.
      Current implementation uses the big-f'ing-lock approach.

    """

    def __init__(self, group=None, target=None, name='theLightweightUploader', args=(), kwargs={}):
        super(LightweightUploader, self).__init__(*args, **kwargs)
        self.daemon = True
        self.upload_queue = []
        self.lock = Lock()

    def enqueue_upload(self, file_name, upload_url, additional_data=None, http_connection=None):
        """
        Add file_object to the upload queue. Returns and upload_id.

        file_object: The file to be uploaded
        upload_url: The absolute URL to which the file should be uploaded
        additional_data: Additional data to be passed to the server in the query string format

        additional_data should be passsed as GET parameters, but is not implemented yet.
        """

        self.lock.acquire(True)
        try:
            if additional_data is not None:
                # I think the way to do this is to encode it into the upload_url, but... ?
                raise NotImplementedError('write me if you want me.')
            id = uuid4()
            url = urlparse(upload_url)
            info('Queueing %s for upload to %s, id: %s', file_name, upload_url, id)
            self.upload_queue.append(UploadQueueEntry(id, UploadableFile(file_name, url, http_connection)))
        finally:
            self.lock.release()
        return id

    def enqueueUpload(self, *args):
        """
        API compatability. Use enqueue_upload instead please.
        """
        self.enqueue_upload(*args)

    def cancel_upload(self, id):
        """
        Cancel the upload with given upload_id. If the upload is in progress, it will be not be continued.
        Upload item is removed from the internal queue.

        upload_id: the id of the upload to be canceled.
        """
        self.lock.acquire(True)
        try:
            self.upload_queue = [x for x in self.upload_queue if x.id != id]
        finally:
            self.lock.release()

    def cancelUpload(self, *args):
        """
        API compatability. Use enqueue_upload instead please.
        """
        self.cancel_upload(*args)

    def set_enabled(self, enabled):
        """
        Not implemented since it appears to be UI related.
        Maybe what this wants to be is a way to pause the uploader?
        """
        raise NotImplementedError('UI functionality is not implemented in this module.')

    def setEnabled(self, *args):
        self.set_enabled(*args)

    def run(self):
        while True:
            self.lock.acquire(True)
            if len(self.upload_queue) < 1:
                self.lock.release()
                debug('Upload queue is empty.')
                sleep(0.1)
                continue
            try:
                top_of_queue = self.upload_queue[0].file
                r = top_of_queue.post_next_chunk()
                if 0 == r:  # finished uploading. Yay!
                    info('Completed uploading %s', top_of_queue.file_name)
                    self.upload_queue = self.upload_queue[1:]
                elif r > 0: # I uploaded a single chunk, carrying on...
                    debug('Uploaded a chunk, continuing to upload %s', top_of_queue.file_name)
                elif r < 0: # Upload of a chunk failed.
                    debug('Failed to upload a chunk.')
                    raise Exception('bonk')
            finally:
                self.lock.release()

    @property
    def is_done(self):
        return ( not self.is_alive() ) or len(self.upload_queue) < 1

class UploadableFile(object):

    # default chunk size is just a guess for now.
    # Note: we are just reading a segment the lenght of chunksize directly into memory.
    # If you set chunk_size to really-really big, you'll run out of memory.
    def __init__(self, file_name, destination_url, http_connection=None, file_type=None, chunk_size=1024*50):
        self._session_id = None
        self._content_length = None
        self._total_file_size = None
        self._file_handle = None
        self.last_byte_uploaded = 0
        self.file_name = file_name
        self._http_connection = http_connection
        self._file_type = file_type
        self.chunk_size = chunk_size
        if isinstance(destination_url, ParseResult):
            self.destination_url = destination_url
        else:
            self.destination_url = urlparse(destination_url)

    @property
    def http_connection(self):
        if self._http_connection is None:
            self._http_connection = HTTPConnection(url.netloc)
        return self._http_connection

    @property
    def session_id(self):
        """
        It appears that this must be numeric. Reference implementation does:
        Math.round(Math.random() * 100000000);
        So clearly, not resumable across multiple sessions, unless we persist session_id to disk...
        """
        if self._session_id is None:
            self._session_id = randint(0, 100000000)
        return self._session_id

    @property
    def file_type(self):
        if self._file_type is None:
            self._file_type = guess_type(self.file_name)[0] or 'application/octet-stream'
            debug('guessing file type: %s', self._file_type)
        return self._file_type

    @property
    def total_file_size(self):
        if self._total_file_size is None:
            self._total_file_size = getsize(self.file_name)
        return self._total_file_size

    @property
    def next_content_range(self):
        plus_chunk = self.last_byte_uploaded + self.chunk_size - 1
        top_bound = plus_chunk if plus_chunk < self.total_file_size else self.total_file_size - 1
        bottom_bound = self.last_byte_uploaded + 1 if self.last_byte_uploaded > 0 else 0
        return 'bytes %d-%d/%d' % (bottom_bound, top_bound, self.total_file_size)

    @property
    def file_handle(self):
        if self._file_handle is None:
            self._file_handle = open(self.file_name, 'rb')
        return self._file_handle

    @property
    def next_chunk(self):
        if self.last_byte_uploaded > 0:
            self.file_handle.seek(self.last_byte_uploaded + 1) # upload starting from the next byte
        chunk = self.file_handle.read(self.chunk_size)
        return chunk

    @property
    def uri_bits(self):
        return '%s?%s' % (self.destination_url.path, self.destination_url.query)

    def post_next_chunk(self):
        from os.path import split
        (head, tail) = split(self.file_name)
        range = self.next_content_range
        headers = {
            'Content-Disposition': 'attachment; filename="%s"' % quote_plus(tail),
            'Content-Type': self.file_type,
            'X-Content-Range': range,
            'Session-ID': self.session_id,
        }

        debug('Sending %s %s', tail, range)
        self.http_connection.request('POST', self.uri_bits, self.next_chunk, headers)
        response = self.http_connection.getresponse()
        debug('Got response: %s', response.read())
        if 201 == response.status:
            # Not done yet, figure out the next lowest bound in the series and set last_byte_uploaded.
            received_range = response.getheader('Range')
            m = RECEIVED_RANGE_PATTERN.match(received_range)
            if m is None:
                debug('Starting at byte 0, since odd received range: %s', received_range)
                self.last_byte_uploaded = 0
            else:
                self.last_byte_uploaded = int(m.group('last_byte_received'))
            return self.total_file_size - self.last_byte_uploaded
        elif 200 == response.status:            # yay! we're done!!!
            self._file_handle.close()
            self.last_byte_uploaded = self.total_file_size
            return 0
        else:
            warning('I got an unexpected return status: %d %s', response.status, response.reason)
            return -1

    @property
    def is_done(self):
        return self.last_byte_uploaded >= self.total_file_size

theLightweightUploader = LightweightUploader()

# This is super-rudimentary. Basically just enough to test with.
if __name__ == '__main__':
    from logging import getLogger, StreamHandler, Formatter, CRITICAL, ERROR, WARNING, INFO, DEBUG
    from optparse import OptionParser
    usage="""usage: %prog destination_url filename1, filename2, ...
    """

    parser = OptionParser(usage=usage, version=__vcs_id__)
    parser.add_option('-q', '--quiet', dest='quiet_count', action='count')
    parser.add_option('-v', '--verbose', dest='verbose_count', action='count')
    (options, arguments) = parser.parse_args()

    console = StreamHandler()
    formatter = Formatter('%(asctime)s %(name)s %(levelname)s %(filename)s:%(lineno)d: %(message)s')
    console.setFormatter(formatter)
    l = getLogger()
    l.addHandler(console)
    raw_log_level = 2   # default to warn level

    if options.verbose_count is not None: raw_log_level += options.verbose_count
    if options.quiet_count is not None:   raw_log_level -= options.quiet_count
    options.ensure_value('verbosity', raw_log_level)
    if   raw_log_level <= 0: l.setLevel(CRITICAL)
    elif raw_log_level == 1: l.setLevel(ERROR)
    elif raw_log_level == 2: l.setLevel(WARNING)    # default
    elif raw_log_level == 3: l.setLevel(INFO)
    else:                    l.setLevel(DEBUG)

    debug('Starting theLightweightUploader thread')
    theLightweightUploader.start()

    # re-use the HTTPConnection rather than making a new one per file
    upload_url = arguments.pop(0)
    url = urlparse(upload_url)

    for f in arguments:
        theLightweightUploader.enqueue_upload(f, upload_url, http_connection=HTTPConnection(url.netloc))

    # wait for all files to be uploaded.
    while not theLightweightUploader.is_done:
        sleep(0.1)
