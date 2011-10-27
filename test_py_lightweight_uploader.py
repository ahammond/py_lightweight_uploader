#!/usr/bin/env python

"""
Relies on the Mock stuff from http://www.voidspace.org.uk/python/mock/
And unittest2 (which is pretty standard these days, seems to me)
"""

from httplib import HTTPConnection, HTTPResponse
from logging import debug, info, warning, critical
from mock import Mock, MagicMock
from patched_unittest2 import *
from random import randint

import py_lightweight_uploader

class TestUploadableFile(PatchedTestCase): pass
@TestUploadableFile.patch('py_lightweight_uploader.debug', spec=debug)
@TestUploadableFile.patch('py_lightweight_uploader.info', spec=info)
@TestUploadableFile.patch('py_lightweight_uploader.warning', spec=warning)
@TestUploadableFile.patch('py_lightweight_uploader.critical', spec=critical)
@TestUploadableFile.patch('py_lightweight_uploader.open', create=True)
@TestUploadableFile.patch('py_lightweight_uploader.randint', spec=randint)
class TestUploadableFile(PatchedTestCase):

    def postSetUpPreRun(self):
        self.mock_randint.return_value = 6543217
        self.mock_open.return_value = MagicMock(spec=file)
        self.mock_open.return_value.tell.return_value = 123456
        self.mock_http_connection = Mock(spec=HTTPConnection)
        self.mock_response = Mock(spec=HTTPResponse)
        self.mock_http_connection.getresponse.return_value = self.mock_response

    def test_is_done_false(self):
        self.target = py_lightweight_uploader.UploadableFile(
            '/path/to/fake_file_name.txt',
            'http://fake.destination/url?a=b&c=d',
            self.mock_http_connection,
        )
        self.assertFalse(self.target.is_done)

    def test_is_done_true(self):
        self.target = py_lightweight_uploader.UploadableFile(
            '/path/to/fake_file_name.txt',
            'http://fake.destination/url?a=b&c=d',
            self.mock_http_connection,
        )
        self.target.next_byte_to_upload = 123456
        self.assertTrue(self.target.is_done)

    def test_destination_file(self):
        self.target = py_lightweight_uploader.UploadableFile(
            '/path/to/fake_file_name.txt',
            'http://fake.destination/url?a=b&c=d',
            self.mock_http_connection,
            destination_filename='fake_destination_file_name.txt'
        )
        self.mock_response.status = 201
        self.mock_response.getheader.return_value = '0-51200/123456'     # what did the server receive?

        self.target.post_next_chunk()
        m = self.mock_http_connection.method_calls
        self.assertEquals({'Content-Disposition': 'attachment; filename="fake_destination_file_name.txt"',
                           'Content-Type': 'text/plain',
                           'Session-ID': 6543217,
                           'X-Content-Range': 'bytes 0-51200/123456'}, m[0][1][3])

    def test_post_next_chunk_testing_first_step(self):
        self.target = py_lightweight_uploader.UploadableFile(
            '/path/to/fake_file_name.txt',
            'http://fake.destination/url?a=b&c=d',
            self.mock_http_connection,
        )
        self.mock_response.status = 201
        self.mock_response.getheader.return_value = '0-51200/123456'     # what did the server receive?

        self.target.post_next_chunk()

        self.assertEquals(51200, self.target.next_byte_to_upload)

        self.mock_open.assert_called_once_with('/path/to/fake_file_name.txt', 'rb')

# TODO: figure out how to test this
#        self.mock_open.return_value.seek.assert_called_once_with(0)
#        self.mock_open.return_value.read.assert_called_once_with(1024*5)

        m = self.mock_http_connection.method_calls
        self.assertEquals(2, len(m))
        self.assertEquals('request', m[0][0])
        self.assertEquals('POST', m[0][1][0])
        self.assertEquals('/url?a=b&c=d', m[0][1][1])
        #self.assertEquals(self.mock_file, m[0][1][2])
        self.assertEquals({'Content-Disposition': 'attachment; filename="fake_file_name.txt"',
                           'Content-Type': 'text/plain',
                           'Session-ID': 6543217,
                           'X-Content-Range': 'bytes 0-51200/123456'}, m[0][1][3])
        self.assertEquals({}, m[0][2])
        self.assertEquals('getresponse', m[1][0])
        self.assertEquals((), m[1][1])
        self.assertEquals({}, m[1][2])

    def test_post_next_chunk_testing_second_step(self):
        self.target = py_lightweight_uploader.UploadableFile(
            '/path/to/fake_file_name.txt',
            'http://fake.destination/url?a=b&c=d',
            self.mock_http_connection,
        )
        self.target.next_byte_to_upload = 10000
        self.mock_response.status = 201
        self.mock_response.getheader.return_value = '0-61200/123456'
        self.target.post_next_chunk()
        self.assertEquals(61200, self.target.next_byte_to_upload)

        self.mock_open.assert_called_once_with('/path/to/fake_file_name.txt', 'rb')

        m = self.mock_http_connection.method_calls
        self.assertEquals(2, len(m))
        self.assertEquals('request', m[0][0])
        self.assertEquals('POST', m[0][1][0])
        self.assertEquals('/url?a=b&c=d', m[0][1][1])
        #self.assertEquals(self.mock_file, m[0][1][2])
        self.assertEquals({'Content-Disposition': 'attachment; filename="fake_file_name.txt"',
                           'Content-Type': 'text/plain',
                           'Session-ID': 6543217,
                           'X-Content-Range': 'bytes 10000-61200/123456'}, m[0][1][3])
        self.assertEquals({}, m[0][2])
        self.assertEquals('getresponse', m[1][0])
        self.assertEquals((), m[1][1])
        self.assertEquals({}, m[1][2])

    def test_post_next_chunk_testing_final_step(self):
        self.target = py_lightweight_uploader.UploadableFile(
            '/path/to/fake_file_name.txt',
            'http://fake.destination/url?a=b&c=d',
            self.mock_http_connection,
        )
        self.target.next_byte_to_upload = 123450
        self.mock_response.status = 200
        self.mock_response.getheader.return_value = '0-123455/123456'
        self.target.post_next_chunk()
        self.assertEquals(123456, self.target.next_byte_to_upload)

        self.mock_open.assert_called_once_with('/path/to/fake_file_name.txt', 'rb')

        m = self.mock_http_connection.method_calls
        self.assertEquals(2, len(m))
        self.assertEquals('request', m[0][0])
        self.assertEquals('POST', m[0][1][0])
        self.assertEquals('/url?a=b&c=d', m[0][1][1])
        #self.assertEquals(self.mock_file, m[0][1][2])
        self.assertEquals({'Content-Disposition': 'attachment; filename="fake_file_name.txt"',
                           'Content-Type': 'text/plain',
                           'Session-ID': 6543217,
                           'X-Content-Range': 'bytes 123450-123455/123456'}, m[0][1][3])
        self.assertEquals({}, m[0][2])
        self.assertEquals('getresponse', m[1][0])
        self.assertEquals((), m[1][1])
        self.assertEquals({}, m[1][2])

    def test_on_complete_go_right(self):
        mock_on_complete = Mock()
        self.target = py_lightweight_uploader.UploadableFile(
            '/path/to/fake_file_name.txt',
            'http://fake.destination/url?a=b&c=d',
            self.mock_http_connection,
            on_complete=mock_on_complete
        )
        self.mock_response.status = 200
        self.mock_response.getheader.return_value = '0-123455/123456'
        self.target.post_next_chunk()
        mock_on_complete.assert_called_once_with(response=self.mock_response)

    def test_on_complete_go_wrong(self):
        mock_on_complete = Mock()
        self.target = py_lightweight_uploader.UploadableFile(
            '/path/to/fake_file_name.txt',
            'http://fake.destination/url?a=b&c=d',
            self.mock_http_connection,
            on_complete=mock_on_complete
        )
        self.mock_response.status = 400
        self.mock_response.reason = 'fake reason to 400'
        self.mock_response.getheader.return_value = ''
        self.target.post_next_chunk()
        mock_on_complete.assert_called_once_with(response=self.mock_response)

    def test_on_complete_not_called_yet(self):
        mock_on_complete = Mock()
        self.target = py_lightweight_uploader.UploadableFile(
            '/path/to/fake_file_name.txt',
            'http://fake.destination/url?a=b&c=d',
            self.mock_http_connection,
            on_complete=mock_on_complete
        )
        self.mock_response.status = 201
        self.mock_response.getheader.return_value = ''
        self.target.post_next_chunk()
        self.assertEquals(False, mock_on_complete.called)


class TestLightweightUploader(PatchedTestCase): pass
@TestLightweightUploader.patch('py_lightweight_uploader.debug', spec=debug)
@TestLightweightUploader.patch('py_lightweight_uploader.info', spec=info)
@TestLightweightUploader.patch('py_lightweight_uploader.warning', spec=warning)
@TestLightweightUploader.patch('py_lightweight_uploader.critical', spec=critical)
class TestLightweightUploader(PatchedTestCase):

    def postSetUpPreRun(self):
        self.mock_file = Mock(spec=py_lightweight_uploader.UploadableFile)

    # The following two tests know entirely too much about the internal implementation of the LWU. :(

    def test_enqueue_upload(self):
        target = py_lightweight_uploader.LightweightUploader()
        id = target.enqueue_upload('fake_filename', 'fake_uploadurl')
        self.assertEquals(1, len(target.upload_queue))
        self.assertEquals(id, target.upload_queue[0].id)

    def test_cancel_upload(self):
        target = py_lightweight_uploader.LightweightUploader()
        id = target.enqueue_upload('fake_filename', 'fake_uploadurl')
        target.cancel_upload(id)
        self.assertEquals(0, len(target.upload_queue))

#    @patch.object(py_lightweight_uploader.UploadableFile, 'post_next_chunk')
#    def test_run_partial_upload(self, mock_post_next_chunk):
#        mock_post_next_chunk.return_value = 1
#        target = py_lightweight_uploader.LightweightUploader()
#        id = target.enqueue_upload('fake_filename', 'fake_uploadurl')
#        target.run()
#
#        a = mock_post_next_chunk.call_args_list
#        self.assertEquals(1, len(a))
#        self.assertEquals((), a[0][0])
#        self.assertEquals({}, a[0][1])
#
#        self.assertEquals(1, len(target.upload_queue))
#
#    @patch.object(py_lightweight_uploader.UploadableFile, 'post_next_chunk')
#    def test_run_finish_upload(self, mock_post_next_chunk):
#        mock_post_next_chunk.return_value = 0
#
#        target = py_lightweight_uploader.LightweightUploader()
#        id = target.enqueue_upload('fake_filename', 'fake_uploadurl')
#        target.run()
#
#        a = mock_post_next_chunk.call_args_list
#        self.assertEquals(1, len(a))
#        self.assertEquals((), a[0][0])
#        self.assertEquals({}, a[0][1])
#
#        self.assertEquals(0, len(target.upload_queue))

