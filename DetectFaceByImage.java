package operation.aws.rekognition;

import adapter.schemas.AssetRecognitionSchema;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.rekognition.AmazonRekognition;
import com.amazonaws.services.rekognition.AmazonRekognitionClientBuilder;
import com.amazonaws.services.rekognition.model.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.ecm.automation.OperationException;
import org.nuxeo.ecm.automation.core.Constants;
import org.nuxeo.ecm.automation.core.annotations.Context;
import org.nuxeo.ecm.automation.core.annotations.Operation;
import org.nuxeo.ecm.automation.core.annotations.OperationMethod;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.api.DocumentModel;
import org.nuxeo.ecm.core.blob.ManagedBlob;
import org.nuxeo.runtime.api.Framework;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class provides functionality for detecting faces in images using AWS Rekognition services.
 */
@Operation(id = DetectFaceByImage.ID, category = Constants.CAT_DOCUMENT, label = "Face Recognition",
        description = "Face Recognition")
public class DetectFaceByImage {

    public static final String ID = "DetectFaceByImage";

    private static final Log logger = LogFactory.getLog(DetectFaceByImage.class);

    private static final String COLLECTION_ID = Framework.getProperty("aws.rekognition.collection.id", "TwelveApostles");
    private static final String BUCKET = Framework.getProperty("nuxeo.s3storage.bucket", "nike-binaries");
    private static final String BUCKET_PREFIX = Framework.getProperty("nuxeo.s3storage.bucket_prefix", "nike-binary/");
    private static final Set<String> ALLOWED_MIME_TYPES = Set.of("image/jpeg", "image/png");

    @Context
    protected CoreSession session;

    @OperationMethod
    public String run(DocumentModel doc) throws OperationException, IOException {
        logger.info("Starting face detection process.");
        validateDocument(doc);

        ManagedBlob blob = (ManagedBlob) doc.getPropertyValue("file:content");
        validateBlob(blob);
        Boolean isSupportedMimeType = validateMimeType(blob.getMimeType());
        if (!isSupportedMimeType) {
            logger.warn("Unsupported MIME type, getting other renditions: " + blob.getMimeType());
            blob = getValidBlob(doc);
        }

        String mimeType = blob.getMimeType(); // Get MIME type of the original image

        String photo = BUCKET_PREFIX + blob.getDigest();
        AmazonS3 s3Client = getS3Client();
        BufferedImage image = fetchImageFromS3(s3Client, photo);

        AmazonRekognition rekognitionClient = createRekognitionClient();
        List<FaceDetail> faceDetails = detectFaces(rekognitionClient, photo);

        if (faceDetails.isEmpty()) {
            logger.info("No faces detected in the image.");
            return ObjectDetectionResult.NO_FACE.getMessage();
        }

        int faceCount = processFaces(rekognitionClient, doc, faceDetails, image, mimeType);

        return faceCount > 0 ? ObjectDetectionResult.DETECTED.getMessage() : ObjectDetectionResult.NOT_DETECTED.getMessage();
    }

    private void validateDocument(DocumentModel doc) throws OperationException {
        if (doc == null || !doc.hasFacet("Picture")) {
            throw new OperationException("Document is not valid or not a picture.");
        }
    }

    private void validateBlob(ManagedBlob blob) throws OperationException {
        if (blob == null) {
            throw new OperationException("File content is null.");
        }
    }

    /**
     * Validates the MIME type of a given file against a predefined whitelist of allowed MIME types.
     *
     * @param mimeType The MIME type of the file to validate.
     * @return true if the MIME type is in the allowed list, false otherwise.
     *
     * Purpose:
     * This method ensures that only files with safe and supported MIME types
     * (e.g., "image/jpeg", "image/png") are processed, reducing the risk of
     * handling potentially harmful or unsupported file types.
     */
    private Boolean validateMimeType(String mimeType) {
        return mimeType != null && ALLOWED_MIME_TYPES.contains(mimeType);
    }

    private ManagedBlob getValidBlob(DocumentModel doc) {
        ManagedBlob fullHDBlob = getFullHDBlob(doc);
        return fullHDBlob != null ? fullHDBlob : (ManagedBlob) doc.getPropertyValue("file:content");
    }

    private ManagedBlob getFullHDBlob(DocumentModel doc) {
        if (doc.hasSchema("picture") && doc.getProperty("picture:views") != null) {
            List<Map<String, Object>> views = (List<Map<String, Object>>) doc.getProperty("picture:views").getValue();
            for (Map<String, Object> view : views) {
                if ("FullHD".equals(view.get("title"))) {
                    return (ManagedBlob) view.get("content");
                }
            }
        }
        return null;
    }

    private AmazonRekognition createRekognitionClient() {
        BasicAWSCredentials awsCreds = getAWSCredentials();
        return AmazonRekognitionClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
                .withRegion(Regions.US_EAST_2)
                .build();
    }

    private BasicAWSCredentials getAWSCredentials() {
        return new BasicAWSCredentials(
                Framework.getProperty("nuxeo.rekognition.awsid", ""),
                Framework.getProperty("nuxeo.rekognition.awssecret", "")
        );
    }

    private AmazonS3 getS3Client() {
        return AmazonS3ClientBuilder.standard()
                .withRegion(Regions.US_EAST_2)
                .withCredentials(new AWSStaticCredentialsProvider(getAWSCredentials()))
                .withClientConfiguration(new ClientConfiguration())
                .build();
    }

    private BufferedImage fetchImageFromS3(AmazonS3 s3Client, String photo) throws IOException {
        try (S3ObjectInputStream inputStream = s3Client.getObject(BUCKET, photo).getObjectContent()) {
            BufferedImage image = ImageIO.read(inputStream);
            if (image == null) {
                throw new IOException("Failed to decode image from S3 object: " + photo);
            }
            return image;
        } catch (IOException e) {
            logger.error("Error reading image from S3 (photo: " + photo + "): " + e.getMessage(), e);
            throw e;
        }
    }

    private List<FaceDetail> detectFaces(AmazonRekognition rekognitionClient, String photo) throws AmazonRekognitionException {
        DetectFacesRequest request = new DetectFacesRequest()
                .withImage(new Image().withS3Object(new S3Object().withName(photo).withBucket(BUCKET)))
                .withAttributes(Attribute.ALL);

        return rekognitionClient.detectFaces(request).getFaceDetails();
    }

    private int processFaces(AmazonRekognition rekognitionClient, DocumentModel doc, List<FaceDetail> faceDetails, BufferedImage image, String mimeType) throws IOException {
        List<UserMatch> userMatchList = faceDetails.parallelStream()
                .map(face -> {
                    try {
                        ByteBuffer croppedFaceBytes = cropFace(image, face.getBoundingBox(), mimeType);
                        if (croppedFaceBytes == null) {
                            logger.warn("Failed to crop face. Skipping...");
                            return null;
                        }
                        return searchUsersByImage(rekognitionClient, croppedFaceBytes);
                    } catch (Exception e) {
                        logger.error("Error processing face: " + e.getMessage(), e);
                        return null;
                    }
                })
                .filter(userMatch -> userMatch != null && userMatch.getUser() != null)
                .toList();

        saveUserMatches(doc, userMatchList);
        return userMatchList.size();
    }

    private UserMatch searchUsersByImage(AmazonRekognition rekognitionClient, ByteBuffer croppedFaceBytes) throws AmazonRekognitionException {
        SearchUsersByImageRequest request = new SearchUsersByImageRequest()
                .withCollectionId(COLLECTION_ID)
                .withImage(new Image().withBytes(croppedFaceBytes))
                .withMaxUsers(1)
                .withUserMatchThreshold(80F);

        List<UserMatch> matches = rekognitionClient.searchUsersByImage(request).getUserMatches();
        return matches.isEmpty() ? null : matches.get(0);
    }

    private void saveUserMatches(DocumentModel doc, List<UserMatch> userMatchList) {
        List<String> userIds = new ArrayList<>();
        for (UserMatch user : userMatchList) {
            userIds.add(user.getUser().getUserId());
        }
        doc.setPropertyValue(AssetRecognitionSchema.CHURCHLEADERS.getPrefixedName(), (Serializable) userIds);
        session.saveDocument(doc);
    }

    private ByteBuffer cropFace(BufferedImage image, BoundingBox box, String mimeType) {
        String formatName = "jpg"; // Default format
        if ("image/png".equals(mimeType)) {
            formatName = "png";
        }
        try {
            int left = (int) (box.getLeft() * image.getWidth());
            int top = (int) (box.getTop() * image.getHeight());
            int width = (int) (box.getWidth() * image.getWidth());
            int height = (int) (box.getHeight() * image.getHeight());

            BufferedImage croppedFace = image.getSubimage(left, top, width, height);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ImageIO.write(croppedFace, formatName, baos);
            byte[] faceBytes = baos.toByteArray();

            if (faceBytes.length == 0) {
                logger.error("Cropped face image is empty.");
                return null;
            }

            return ByteBuffer.wrap(faceBytes);
        } catch (IOException e) {
            logger.error("Error occurred while cropping face: " + e.getMessage(), e);
            return null;
        }
    }
}