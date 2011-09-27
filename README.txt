


Author: Andrew Hammond <andrew.hammond@receipt.com>
Copyright (c) SmartReceipt Inc. 2011

Requirements:
- Uses python's Threading module for the upload manager.
It would be really cool to abstract this out!

Usage:

from lightweight_upload import getLightWeightUploader
lwu = getLightWeightUploader()      # instantiates an
# register callbacks here, for example
lwu.onProgress(
lwu.init()
lwu.enqueueUpload(file_object, upload_url, additional_data)



