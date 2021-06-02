import numpy as np
import cv2
import time
import os
os.environ['CUDA_VISIBLE_DEVICES'] = '-1'
os.environ['TZ'] = 'UTC'
time.tzset()
from datetime import datetime
from pymongo import MongoClient
from kafka import KafkaConsumer
from kafka import KafkaProducer



import sys
import base64
import json
from json import loads
from PIL import Image
from io import BytesIO

topic = "leonardo-stream"
#topic = "leonardo-stream-5"
producer_topic = "leonardo-stream-2"


consumer = KafkaConsumer(
     topic,
     bootstrap_servers=['10.0.10.1:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group',
     value_deserializer=lambda x: loads(x.decode('utf-8'))
     )
     

     
# Start up producer
producer = KafkaProducer(bootstrap_servers='10.0.10.1:9092',
compression_type='gzip',
linger_ms=5
)



conf = 0.4 #confidence

prototxt = "/app/MobileNetSSD_deploy.prototxt.txt"
model = "/app/MobileNetSSD_deploy.caffemodel" 


CLASSES = ["background", "aeroplane", "bicycle", "bird", "boat",
     "bottle", "bus", "car", "cat", "chair", "cow", "diningtable",
     "dog", "horse", "motorbike", "person", "pottedplant", "sheep",
     "sofa", "train", "tvmonitor"]
COLORS = np.random.uniform(0, 255, size=(len(CLASSES), 3))
print("[INFO] loading model...")
net = cv2.dnn.readNetFromCaffe(prototxt, model)



def stringToRGB(base64_string):
    imgdata = base64.b64decode(str(base64_string))
    image = Image.open(BytesIO(imgdata))
    return cv2.cvtColor(np.array(image), cv2.COLOR_BGR2RGB)   

def nms(detections, threshold=.5):
   
    detections = sorted(detections, key=lambda detections: detections[2],
            reverse=True)
    
    new_detections=[]
    
    new_detections.append(detections[0])
    
    del detections[0]
    
    for index, detection in enumerate(detections):
        for new_detection in new_detections:
            if overlapping_area(detection, new_detection) > threshold:
                del detections[index]
                break
        else:
            new_detections.append(detection)
            del detections[index]
    return new_detections

def overlapping_area(detection_1, detection_2):
    
    x1_tl = detection_1[0]
    x2_tl = detection_2[0]
    x1_br = detection_1[0] + detection_1[3]
    x2_br = detection_2[0] + detection_2[3]
    y1_tl = detection_1[1]
    y2_tl = detection_2[1]
    y1_br = detection_1[1] + detection_1[4]
    y2_br = detection_2[1] + detection_2[4]
    # Calculate the overlapping Area
    x_overlap = max(0, min(x1_br, x2_br)-max(x1_tl, x2_tl))
    y_overlap = max(0, min(y1_br, y2_br)-max(y1_tl, y2_tl))
    overlap_area = x_overlap * y_overlap
    area_1 = detection_1[3] * detection_2[4]
    area_2 = detection_2[3] * detection_2[4]
    total_area = area_1 + area_2 - overlap_area
    return overlap_area / float(total_area)
    

def correct_encoding(dictionary):
    """Correct the encoding of python dictionaries so they can be encoded to mongodb
    inputs
    -------
    dictionary : dictionary instance to add as document
    output
    -------
    new : new dictionary with (hopefully) corrected encodings"""

    new = {}
    for key1, val1 in dictionary.items():
        # Nested dictionaries
        if isinstance(val1, dict):
            val1 = correct_encoding(val1)

        if isinstance(val1, np.bool_):
            val1 = bool(val1)

        if isinstance(val1, np.int64):
            val1 = int(val1)

        if isinstance(val1, np.float64):
            val1 = float(val1)
            
        if isinstance(val1, set):
            vall = list(val1)

        new[key1] = val1

    return new
    
    
def send_kafka(tempo,mongoid):
    x = { 
        "mongoid":mongoid,        
        "timestamp":tempo,
    }
    y=json.dumps(x)
    producer.send(producer_topic, y.encode('utf-8'))    

myclient = MongoClient("mongodb://10.0.10.1:27017/")
mydb = myclient["leonardo"]
mycol = mydb["leonardostream"]


frames = 0
start_time = time.time()
person = 0


#out = cv2.VideoWriter('preview.avi',cv2.VideoWriter_fourcc('M','J','P','G'), 10, (720,480)) 

path_resultado = "images"
if os.path.isdir(path_resultado) == False:
    os.mkdir(path_resultado)
    
nome_video = "none"


for message in consumer:
    
    

    #continue
    startt = time.time()
    
    message = message.value
    
    if (nome_video != message["name"]):
        nome_video = message["name"]
        frames = 0
    
    frame = stringToRGB(message['data'])
    
    name = message['name']
    
    #print(name)
    #print(message['timestamp'])
    

    (h, w) = frame.shape[:2]
    blob = cv2.dnn.blobFromImage(cv2.resize(frame, (300, 300)), 0.007843, (300, 300), 127.5)
    #print("[INFO] computing object detections…")
    net.setInput(blob)
    detections = net.forward()
    #detections = nms(detections, 0.2) 
    
    
    for i in np.arange(0, detections.shape[2]):
         confidence = detections[0, 0, i, 2]
         
         if confidence > conf:            
            idx = int(detections[0, 0, i, 1])
            if idx == 15: 
                person +=1
                box = detections[0, 0, i, 3:7] * np.array([w, h, w, h])
                (startX, startY, endX, endY) = box.astype("int")
                label = "{}: {:.2f}%".format(CLASSES[idx], confidence * 100)     
                #print("[INFO] {}".format(label))
                
                    
                
                cv2.rectangle(frame, (startX, startY), (endX, endY),COLORS[idx], 2)     
                y = startY - 15 if startY - 15 > 15 else startY + 15     
                cv2.putText(frame, label, (startX, y),
                cv2.FONT_HERSHEY_SIMPLEX, 0.5, COLORS[idx], 2)
                
                #roi = img[c1[0]:c1[1],c2[0]:c2[1]]
                #roi = frame[startY:endY,startX:endX]
                
                new_insert={"personid":person,
                         "frameid": frames,
                         "data": message['data'],
                         "videoid":message['name'],
                         "bbox": {"StartX":startX, "StartY":startY, "EndX":endX, "EndY":endY},
                         "timestamp": str(datetime.now()),
                         "video_time": message['time']}
                         
                new_insert = correct_encoding(new_insert)
                         
                idfind=mycol.insert_one(new_insert) 
                
                send_kafka(message['time'],str(idfind.inserted_id))
    
                 #try:
                 #    cv2.imwrite(path_resultado+'/'+str(idfind)+".jpg", roi)
                #except:
                #    continue
          
                #print(str(idfind))   


    #end_time = time.time()
    frames += 1
    #print("FPS of the video is {:5.2f}".format( frames / (end_time - start_time)))
    print("Seconds since epoch =", (time.time() - startt)% 60 )
    #print(frames)

    
    person = 0
            
            

        #out.write(frame)

    
    

