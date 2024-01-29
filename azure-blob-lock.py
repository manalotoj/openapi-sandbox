from azure.storage.blob import BlobServiceClient, BlobLeaseClient
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
import azure.functions as func
import time

class DistributedLock:
    def __init__(self, blob_client, lease_duration=15):
        self.blob_client = blob_client
        self.lease_duration = lease_duration
        self.lease_client = BlobLeaseClient(blob_client)
        self.lease_id = None

    def acquire(self):
        try:
            # Ensure the blob exists, create if it does not
            try:
                self.blob_client.upload_blob(b'', overwrite=False)
            except ResourceExistsError:
                pass

            # Try to acquire the lease
            self.lease_id = self.lease_client.acquire(lease_duration=self.lease_duration)
            return True
        except ResourceNotFoundError:
            # Blob doesn't exist
            return False
        except Exception as e:
            # Handle other exceptions
            print(f"Error acquiring lock: {e}")
            return False

    def release(self):
        if self.lease_id:
            try:
                self.lease_client.release()
            except Exception as e:
                print(f"Error releasing lock: {e}")

    def renew(self):
        if self.lease_id:
            try:
                self.lease_client.renew()
            except Exception as e:
                print(f"Error renewing lock: {e}")

def main(req: func.HttpRequest) -> func.HttpResponse:
    # Initialize Azure Blob Service Client
    blob_service_client = BlobServiceClient.from_connection_string('your_connection_string')
    container_client = blob_service_client.get_container_client('your_container')
    blob_client = container_client.get_blob_client('lock_blob')

    lock = DistributedLock(blob_client)

    if lock.acquire():
        try:
            # Perform operations here
            # ...

            # If operation takes longer than lease duration, renew the lease
            # lock.renew()

            return func.HttpResponse("Operation completed")
        except Exception as e:
            return func.HttpResponse(f"Operation failed: {str(e)}", status_code=500)
        finally:
            lock.release()
    else:
        return func.HttpResponse("Could not acquire lock", status_code=429)

# Additional functions as needed...
