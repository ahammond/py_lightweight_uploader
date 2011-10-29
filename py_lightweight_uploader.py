#!/usr/bin/env python

from cStringIO import StringIO
from httplib import HTTPConnection, HTTPSConnection
from logging import debug, info, warning, critical
from mimetypes import guess_type
from os import SEEK_END
from random import randint
import re
from threading import Thread, Lock
from time import sleep
from urllib import quote_plus, urlencode
from urlparse import urlparse, ParseResult, urlunparse
from uuid import uuid4

__author__ = 'Andrew Hammond <andrew.hammond@receipt.com>'
__copyright__ = 'Copyright (c) 2011 SmartReceipt'
__license__ = """Standard 2 clause BSD (matches nginx license)
Redistribution and use in source and binary forms, with or without modification, are
permitted provided that the following conditions are met:

   1. Redistributions of source code must retain the above copyright notice, this list of
      conditions and the following disclaimer.

   2. Redistributions in binary form must reproduce the above copyright notice, this list
      of conditions and the following disclaimer in the documentation and/or other materials
      provided with the distribution.

THIS SOFTWARE IS PROVIDED BY SmartReceipt Inc. ''AS IS'' AND ANY EXPRESS OR IMPLIED
WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL SmartReceipt Inc. OR
CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

The views and conclusions contained in the software and documentation are those of the
authors and should not be interpreted as representing official policies, either expressed
or implied, of SmartReceipt Inc.
"""
__vcs_id__ = '$Id$'


# Note: this pattern recognizes the first contiguous segment of the file.
# There may be other segments already uploaded.
# In that case, we ignore the subsequent segments and simply find the first
# MISSING segment. This doesn't necessarily mean we will re-upload anything,
# since, after uploading the first missing segment, we check again.
RECEIVED_RANGE_PATTERN = re.compile(r'^0-(?P<next_byte_to_upload>\d+)')

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
        super(LightweightUploader, self).__init__(group=group, target=target, name=name, *args, **kwargs)
        self.daemon = True
        self.upload_queue = []
        self.lock = Lock()

    def enqueue_upload(self,
                       file_name,
                       upload_url,
                       additional_data=None,
                       http_connection=None,
                       destination_filename=None,
                       on_complete=None,
                       content=None
            ):
        """
        Add file_object to the upload queue. Returns an upload_id.

        file_name: The name file to be uploaded
        upload_url: The absolute URL to which the file should be uploaded
        additional_data: Additional data to be passed to the server in the query string as GET parameters
        http_connection: optional, but if given, the HttpConnection object to use for sending
        destination_filename: name file should be uploaded to. If None, defaults to current filename.
        on_complete: called when the upload completes and given response=HttpResponse object.
        content: override the content of the file. If a seekable/readable object, treat as filehandle.
          Otherwise, treat it as a string.
        """

        self.lock.acquire(True)
        try:
            id = uuid4()
            url = urlparse(upload_url)
            if additional_data is not None:
                updated_query = url.query + '&' if url.query else ''
                updated_query += urlencode(additional_data)
                url = ParseResult(
                        scheme=url.scheme,
                        netloc=url.netloc,
                        path=url.path,
                        params=url.params,
                        query= updated_query,
                        fragment=url.fragment
                    )

            info('Queueing %s for upload to %s, id: %s', file_name, upload_url, id)
            self.upload_queue.append(
                UploadQueueEntry(
                    id,
                    UploadableFile(
                        file_name,
                        url,
                        http_connection=http_connection,
                        destination_filename=destination_filename,
                        on_complete=on_complete,
                        content=content
                    )
                )
            )
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

    """
    The default chunk size is just a guess for now.
    Note: we are just reading a segment the lenght of chunksize directly into memory.
    If you set chunk_size to really-really big, you might have memory issues.

    Content of what is uploaded is determined in file_handle() below.
    If the content is not None, the it is used as the source of the file's contents.
    If it is a string, it is turned into a StringIO. Otherwise it is simply treated as a file type object.

    """
    # TODO: Have a boundary size (probably related to chunk size in some way) and do a simple post for smaller files?

    def __init__(self,
                 file_name,
                 destination_url,
                 http_connection=None,
                 destination_filename=None,
                 file_type=None,
                 chunk_size=None,
                 on_complete=None,
                 content=None
            ):
        self._session_id = None
        self._content_length = None
        self._total_file_size = None
        self._file_handle = None
        self.next_byte_to_upload = 0
        self.file_name = file_name
        self._http_connection = http_connection
        self._destination_filename = destination_filename
        self._file_type = file_type
        self.chunk_size = chunk_size if chunk_size is not None else 1024*50
        self.on_complete = on_complete
        self.content = content
        self.response = None
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
            self.file_handle.seek(0, SEEK_END)
            self._total_file_size = self.file_handle.tell()
        return self._total_file_size

    @property
    def next_content_range(self):
        plus_chunk = self.next_byte_to_upload + self.chunk_size
        top_bound = plus_chunk if plus_chunk < self.total_file_size else self.total_file_size - 1
        return 'bytes %d-%d/%d' % (self.next_byte_to_upload, top_bound, self.total_file_size)

    @property
    def file_handle(self):
        if self._file_handle is None:
            if self.content is None:
                self._file_handle = open(self.file_name, 'rb')
            else:
                self._file_handle = self.content        # assume it's a file handle / StringIO
                try:
                    self.content.seek(0)
                except AttributeError:                  # assume it's a string
                    self._file_handle = StringIO(self.content)
        return self._file_handle

    @property
    def next_chunk(self):
        self.file_handle.seek(self.next_byte_to_upload) # upload starting from the next byte
        chunk = self.file_handle.read(self.chunk_size + 1)
        return chunk

    @property
    def destination_filename(self):
        if self._destination_filename is None:
            from os.path import split
            (head, tail) = split(self.file_name)
            self._destination_filename = tail
        return self._destination_filename

    @property
    def uri(self):
        return '%s?%s' % (self.destination_url.path, self.destination_url.query)

    def post_next_chunk(self):
        range = self.next_content_range
        headers = {
            'Content-Disposition': 'attachment; filename="%s"' % quote_plus(self.destination_filename),
            'Content-Type': self.file_type,
            'X-Content-Range': range,
            'Session-ID': self.session_id,
        }

        debug('Sending %s %s', self.destination_filename, range)
        self.http_connection.request('POST', self.uri, self.next_chunk, headers)
        self.response = self.http_connection.getresponse()
        debug('Got response: %s', self.response.read())
        if 201 == self.response.status:
            # Not done yet, figure out the next lowest bound in the series and set next_byte_to_upload.
            received_range = self.response.getheader('Range')
            m = RECEIVED_RANGE_PATTERN.match(received_range)
            if m is None:
                debug('Starting at byte 0, since odd received range: %s', received_range)
                self.next_byte_to_upload = 0
            else:
                self.next_byte_to_upload = int(m.group('next_byte_to_upload'))
                debug('Advancing next_byte_to_upload to %d', self.next_byte_to_upload)
            return self.total_file_size - self.next_byte_to_upload
        elif 200 == self.response.status:            # yay! we're done!!!
            self._file_handle.close()
            self.next_byte_to_upload = self.total_file_size
            if self.on_complete:
                self.on_complete(response=self.response)
            return 0
        # TODO: add redirection support?
#        elif self.response.status in (301, 307): # perm/temp redir
#            new_url = self.response.headers['Location']
        else:
            warning('I got an unexpected return status: %d %s', self.response.status, self.response.reason)
            if self.on_complete:
                self.on_complete(response=self.response)
            return -1

    @property
    def is_done(self):
        return self.next_byte_to_upload >= self.total_file_size

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
    scheme = url.scheme.lower()
    if 'http' == scheme:
        connection = HTTPConnection(url.netloc)
    elif 'https' == scheme:
        connection = HTTPSConnection(url.netloc)
    else:
        raise ValueError("I only know how to upload via either http https")

    for f in arguments:
        def notify(response):
            info('%s: %d %s', f, response.status, response.reason)
        theLightweightUploader.enqueue_upload(
            f,
            upload_url,
            http_connection=connection,
            on_complete=notify)

    # wait for all files to be uploaded.
    while not theLightweightUploader.is_done:
        sleep(0.1)
