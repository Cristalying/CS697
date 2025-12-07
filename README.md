# CS697 – AI-Based Image Tagging & Face Recognition System

This repository contains the implementation, demo videos, and batch processing scripts used for the AI-based facial recognition and landmark image tagging system. The project integrates AWS Rekognition, Lambda, SQS, and S3 to enable automated, scalable, and cost-efficient image analysis.

---

## 📌 Project Structure & File Descriptions

### **1. AnalyseImageFunction.py**
Lambda function used for analyzing landmark images in batch mode.  
It retrieves images from S3, extracts features using AWS Rekognition, and writes metadata back to storage.

---

### **2. DetectFaceByImage.java**
Java-based facial detection module.  
This function receives an input image, detects a human face, extracts embeddings, and passes results downstream for matching.

---

### **3. Face recognition-AWS.mov**
Demo video showcasing the real-time **face recognition** workflow:
- Upload image  
- Automatic face detection and embedding extraction  
- AWS Rekognition match lookup  
- Metadata stored into S3/Nuxeo  
- Result returned with confidence score  

---

### **4. LandMark recognition-AWS.mov**
Demo video demonstrating **batch landmark image recognition**, including:
- S3 image ingestion  
- Batch queueing via SQS  
- AWS Rekognition label extraction  
- Automatic metadata tagging  

---

### **5. StartModelFunction.py**
Script that initializes and starts the AWS Rekognition Custom Labels model before batch processing begins.

---

### **6. StopModelFunction.py**
Stops the Rekognition Custom Labels model after batch processing to reduce cost.

---

### **7. fetchObjectsAndCreateSQSMessages.py**
Fetches unprocessed images from S3 and creates SQS messages for batch processing.  
Acts as the message producer in the distributed processing pipeline.

---

### **8. sqs_poller.py**
SQS message consumer.  
Polls SQS, invokes processing functions, and coordinates status updates.

---

---

## 🎥 Demo Videos
| Feature | Video Link |
|--------|------------|
| Face Recognition | https://github.com/Cristalying/CS697/blob/main/Face%20recognition-AWS.mov |
| Landmark Batch Recognition | https://github.com/Cristalying/CS697/blob/main/LandMark%20recognition-AWS.mov |

---

## 📊 Dataset Sources
- https://www.churchofjesuschrist.org/learn/quorum-of-the-twelve-apostles?lang=eng  
- https://www.churchofjesuschrist.org/media/collection/temples-images?lang=eng

---

## 🚀 Final Deliverables
- Functional automated face recognition system  
- Batch landmark processing workflow  
- Scalable AWS-based architecture  
- Demo videos & complete source code

---

## ✅ Conclusion
All goals defined in the midterm and final project have been achieved.  
The system is accurate, scalable, and cost-efficient, and is ready for real-world integration.
