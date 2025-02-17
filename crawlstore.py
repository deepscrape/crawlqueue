async def updateCrawlOperation(userId, operation_id, data, db):
    try:
        # Update Firestore document
        doc_ref = db.collection(f"users/{userId}/operations").document(operation_id)
        doc_ref.update(data)
        print(
            f"\033[94mINFO:\033[0m     \033[92mDocument updated with ID: {operation_id}\033[0m"
        )
    except Exception as e:
        print(
            f"\033[91mERROR:\033[0m     \033[93mFailed to update document: {e}\033[0m"
        )


async def getCrawlMetadata(metadataId, userId, db):
    """

    Asynchronously updates a Firestore document for a user's crawl operation.

    Parameters:

    userId (str): The ID of the user whose operation document is to be updated.
    metadataId (str): The ID of the metadata document to update. This document contains configuration settings for the crawl operation.

    Logs:

    Prints a success message if the gets is successful.

    Prints an error message if the get fails.

    """
    try:
        # Get Firestore document
        doc_ref = db.collection(f"users/{userId}/crawlconfigs").document(metadataId)
        doc = doc_ref.get()
        if doc.exists:
            print(
                f"\033[94mINFO:\033[0m     \033[92mDocument retrieved with ID: {metadataId}\033[0m"
            )
            return doc.to_dict()
        else:
            print(
                f"\033[91mERROR:\033[0m     \033[93mDocument not found with ID: {metadataId}\033[0m"
            )
            return None
    except Exception as e:
        print(
            f"\033[91mERROR:\033[0m     \033[93mFailed to retrieve document: {e}\033[0m"
        )
        return None
