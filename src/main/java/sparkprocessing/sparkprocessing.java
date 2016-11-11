package sparkprocessing;

import java.io.*;  
import java.sql.*;  
import java.util.*;  
  
import kafka.serializer.DefaultDecoder;  
import kafka.serializer.StringDecoder;  
import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.io.IOUtils;  
import org.apache.hadoop.io.SequenceFile;  
import org.opencv.core.*;  
import org.opencv.face.Face;  
import org.opencv.face.FaceRecognizer;  
import org.opencv.imgproc.Imgproc;  
import scala.Tuple2;  
  
  
import org.apache.hadoop.io.BytesWritable;  
import org.apache.hadoop.io.Text;  
import org.apache.kafka.clients.producer.KafkaProducer;  
import org.apache.kafka.clients.producer.Producer;  
import org.apache.kafka.clients.producer.ProducerRecord;  
  
/*import org.apache.kafka.clients.consumer.ConsumerConfig; 
import org.apache.kafka.clients.consumer.KafkaConsumer; 
import org.apache.kafka.clients.consumer.ConsumerRecord; 
import org.apache.kafka.clients.consumer.ConsumerRecords;*/  
  
  
import org.apache.spark.SparkConf;  
import org.apache.spark.api.java.function.Function;  
import org.apache.spark.api.java.function.PairFunction;  
import org.apache.spark.streaming.api.java.*;  
import org.apache.spark.streaming.kafka.KafkaUtils;  
import org.opencv.imgcodecs.Imgcodecs;  
import org.opencv.objdetect.CascadeClassifier;  
import org.apache.spark.streaming.Durations;  
  
public class sparkprocessing {  
    static Producer<String, String> producer;  
    static Map<Integer, String> idToNameMapping;  
    static FaceRecognizer faceRecognizer;  
    static MatOfInt labelsBuf;  
    static List<Mat> mats;  
    static Map<String, Mat> lableMat = new HashMap<String, Mat>();  
  
    @SuppressWarnings("serial")  
    public static class ConvertToWritableTypes implements PairFunction<Tuple2<String, byte[]>, Text, BytesWritable> {  
        @SuppressWarnings({"unchecked", "rawtypes"})  
        public Tuple2<Text, BytesWritable> call(Tuple2<String, byte[]> record) {  
            return new Tuple2(new Text(record._1), new BytesWritable(record._2));  
        }  
    }  
    public static void train(){  
        try {  
            System.loadLibrary(Core.NATIVE_LIBRARY_NAME);  
  
            String uri = "hdfs://10.75.161.88/stars.seq";  
//            String uri = "hdfs://10.75.161.242:9000/all.seq";  
            mats = new ArrayList<Mat>();  
            idToNameMapping = new HashMap<Integer, String>();  
            Configuration conf = new Configuration();  
            Path path = new Path(uri);  
            SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(path));  
//            System.out.println("1");  
            Map<Text, OutputStream> keyStream = new HashMap<Text, OutputStream>();  
            Text key = new Text();  
            Text value = new Text();  
            int count = 0;  
            while (reader.next(key, value)) {  
                if (!idToNameMapping.containsValue(key.toString().split("_")[0])) {  
                    idToNameMapping.put(count++, key.toString().split("_")[0]);  
                }  
                if (key.toString().trim() != null && !keyStream.containsKey(key)) {  
                    keyStream.put(new Text(key), new ByteArrayOutputStream(1024));  
                }  
                keyStream.get(key).write(value.getBytes(), 0, value.getLength());  
            }  
            Map<String, Integer> nameToId = new HashMap<String, Integer>();  
            for (Map.Entry entry : idToNameMapping.entrySet()) {  
                nameToId.put((String) entry.getValue(), (Integer) entry.getKey());  
            }  
            Mat mat;  
            ByteArrayOutputStream bs = null;  
            int counter = 0;  
            labelsBuf = new MatOfInt(new int[keyStream.size()]);  
            for (Map.Entry out : keyStream.entrySet()) {  
                bs = ((ByteArrayOutputStream) out.getValue());  
                bs.flush();//Imgcodecs.CV_LOAD_IMAGE_GRAYSCALE  
                mat = Imgcodecs.imdecode(new MatOfByte(bs.toByteArray()), Imgcodecs.CV_IMWRITE_JPEG_OPTIMIZE);  
                Mat matSave = Imgcodecs.imdecode(new MatOfByte(bs.toByteArray()), Imgcodecs.CV_LOAD_IMAGE_COLOR);  
                mats.add(detectElements(mat.clone()).get(0));  
                Imgcodecs.imwrite("/tmp/" + new Random().nextInt(100) + ".jpg",detectElements(mat.clone()).get(0));  
                int labelId = nameToId.get(out.getKey().toString().split("_")[0]);  
                lableMat.put(out.getKey().toString().split("_")[0], matSave.clone());  
                labelsBuf.put(counter++, 0, labelId);  
            }  
            IOUtils.closeStream(bs);  
            IOUtils.closeStream(reader);  
  
            faceRecognizer = Face.createFisherFaceRecognizer();  
//         FaceRecognizer faceRecognizer = Face.createEigenFaceRecognizer();  
//         FaceRecognizer faceRecognizer = Face.createLBPHFaceRecognizer();  
  
            faceRecognizer.train(mats, labelsBuf);  
            if(faceRecognizer == null) {  
                System.out.println("in the static after tain, face rec is null");  
            } else {  
                System.out.println("!!!!!!!!face rec is not null");  
            }  
        } catch (Exception e) {  
            System.out.println(e.toString());  
            System.exit(100);  
        }  
    }  
  
    @SuppressWarnings("serial")  
    public static void main(String[] args) throws Exception{  
        System.loadLibrary(Core.NATIVE_LIBRARY_NAME);//54  
        System.out.println("train after");  
        String brokers = "10.75.161.54:9092";  
//        String brokers = "172.16.1.11:9092";  
        String Stringbrokers = "10.75.161.88:9092";  
//      String brokers = "10.140.92.221:9092";  
        String topics = "ddp_video_source";  
  
  
  
        // Create context with a 2 seconds batch interval  
        SparkConf sparkConf = new SparkConf().setAppName("JavaDirectKafkaVideoData");  
        sparkConf.setMaster("local");  
        sparkConf.set("spark.testing.memory", "2147480000");  
  
        final JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));  
  
        // for graceful shutdown of the application ...  
        Runtime.getRuntime().addShutdownHook(new Thread() {  
            @Override  
            public void run() {  
                System.out.println("Shutting down streaming app...");  
                if (producer != null)  
                    producer.close();  
                jssc.stop(true, true);  
                System.out.println("Shutdown of streaming app complete.");  
            }  
        });  
  
        HashSet<String> topicsSet = new HashSet<String>(Arrays.asList(topics.split(",")));  
        HashMap<String, String> kafkaParams = new HashMap<String, String>();  
        kafkaParams.put("metadata.broker.list", brokers);  
        kafkaParams.put("group.id", "groupid");  
        kafkaParams.put("consumer.id", "consumerid");  
//        kafkaParams.put("bootstrap.servers", "cdhmanage");  
        kafkaParams.put("zookeeper.connect", "cdh3:2181/kafka");  
  
        // Create direct kafka stream with brokers and topics  
        JavaPairInputDStream<String, byte[]> messages = KafkaUtils.createDirectStream(  
                jssc,  
                String.class,  
                byte[].class,  
                StringDecoder.class,  
                DefaultDecoder.class,  
                kafkaParams,  
                topicsSet  
        );  
  
        train();  
        JavaDStream<String> content = messages.map(new Function<Tuple2<String, byte[]>, String>() {  
            //@Override  
            public String call(Tuple2<String, byte[]> tuple2) throws IOException {  
                System.loadLibrary(Core.NATIVE_LIBRARY_NAME);  
                String lable = null;  
                if (tuple2 == null) {  
                    System.out.println("null");  
                    return null;  
                } else {  
                    if (tuple2._2().length > 1000) {  
                        Mat image = new Mat(new Size(640, 480), 16);  
                        image.put(0, 0, tuple2._2());  
//                        Imgcodecs.imwrite("/tmp/test"+ new Random().nextInt(100) +".jpg", image);  
                        System.out.println("tuple2._2().length > 1000");  
                        if (detectElements(image.clone()).size() > 0) {  
  
                            Mat person = detectElements(image.clone()).get(0);  
                            System.out.println(person.width() + "person.width");  
                            System.out.println(person.height() + "person.height");  
                            if(faceRecognizer == null) {  
                                System.out.println("after tain, face rec is null");  
                            }  
                            if(person == null) {  
                                System.out.println("person is null");  
                            }  
                            int predictedLabel = faceRecognizer.predict(person);  
//                            Imgcodecs.imwrite("/home/test/person"+ new Random().nextInt(100) +".pgm", person);  
                            System.out.println("Predicted label: " + idToNameMapping.get(predictedLabel));  
                            System.out.println("**********");  
                            System.out.println("**********");  
                            System.out.println("**********");  
                            System.out.println("**********");  
                            try {  
                                Class.forName("com.mysql.jdbc.Driver");  
                                Connection connection = DriverManager.getConnection("jdbc:mysql://localhost/", "root", "root");  
                                Statement statement = connection.createStatement();  
                                statement.executeUpdate("CREATE DATABASE IF NOT EXISTS images;");  
                                statement.executeUpdate("use images;");  
                                statement.executeUpdate("CREATE TABLE IF NOT EXISTS faces (\n" +  
                                        "  id    INT          NOT NULL AUTO_INCREMENT,\n" +  
                                        "  originalImage MediumBlob  NOT NULL,\n" +  
                                        "  timeLabel VARCHAR(100) NOT NULL,\n" +  
                                        "  matchedImage MediumBlob         NOT NULL,\n" +  
                                        "  PRIMARY KEY (id)\n" +  
                                        ")\n" +  
                                        "  ENGINE = InnoDB;");  
                                PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO faces VALUES (NULL, ?, ?, ?)");  
                                Imgcodecs.imwrite("/tmp/1.jpg", image);  
                                preparedStatement.setBinaryStream(1, new FileInputStream("/tmp/1.jpg"));  
//                                new BufferedInputStream(new FileInputStream(new File("")));  
                                preparedStatement.setString(2, "Time:" + System.nanoTime() + ", name:" + idToNameMapping.get(predictedLabel));  
  
                                /*for (Map.Entry kv: idToNameMapping.entrySet() 
                                     ) { 
                                    System.out.println(kv.getKey().toString() + " id"); 
                                    System.out.println(kv.getValue().toString() + " value"); 
                                } 
                                for (Map.Entry kv: lableMat.entrySet() 
                                     ) { 
                                    System.out.println(kv.getKey() + " key"); 
                                    System.out.println("/tmp/value"); 
                                    Imgcodecs.imwrite("/tmp/" + kv.getKey() + ".pgm", (Mat)kv.getValue()); 
                                }*/  
                                Imgcodecs.imwrite("/tmp/2.jpg", lableMat.get(idToNameMapping.get(predictedLabel)));  
                                preparedStatement.setBinaryStream(3, new FileInputStream("/tmp/2.pgm"));  
                                preparedStatement.execute();  
  
                                connection.close();  
                                System.out.println("sql insert, name = " + idToNameMapping.get(predictedLabel));  
                            }  
                            catch (Exception e) {  
                                System.out.println(e.toString());  
                            }  
  
                        } else {  
                            System.out.println("NO person");  
  
                        }  
                    } else {  
                        System.out.println("tuple2._2().length < 1000");  
                    }  
                    return lable;  
                }  
            }  
        });  
        content.count().print();  
  
        // Start the computation  
        jssc.start();  
        jssc.awaitTermination();  
    }  
  
    public static List<Mat> detectElements(Mat inputFrame) {  
//        Imgcodecs.imwrite("/tmp/" + new Random().nextInt(100) + ".pgm", inputFrame);  
        System.loadLibrary(Core.NATIVE_LIBRARY_NAME);  
        List<Mat> detectedElements = new ArrayList<Mat>(10);  
        Mat mRgba = new Mat();  
        Mat mGrey = new Mat();  
        inputFrame.copyTo(mRgba);  
        inputFrame.copyTo(mGrey);  
        MatOfRect results = new MatOfRect();  
        Imgproc.cvtColor( mRgba, mGrey, Imgproc.COLOR_BGR2GRAY);  
        Imgproc.equalizeHist( mGrey, mGrey );  
        CascadeClassifier cascadeClassifier =  
                new CascadeClassifier(GetResourceFilePath("/haarcascade_frontalface_alt.xml"));  
        cascadeClassifier.detectMultiScale(mGrey, results);  
        Rect[] classifiedElements = results.toArray();  
        System.out.println("Dectected person: " + classifiedElements.length);  
  
        for (Rect rect : classifiedElements) {  
            // and adds it to the  
            Mat convert = new Mat();  
            Mat face = new Mat(mRgba.clone(), rect);  
            Imgproc.cvtColor(face, face, Imgproc.COLOR_BGR2GRAY);  
            face.convertTo(convert, Imgproc.COLOR_BGR2GRAY);  
            detectedElements.add(resizeFace(convert));  
//            Imgcodecs.imwrite("/tmp/face" + new Random().nextInt(10)+ ".pgm", resizeFace(convert));  
        }  
        System.out.println("Get fave: " + detectedElements.size());  
        return detectedElements;  
    }  
    public static Mat resizeFace(Mat originalImage) {  
        System.loadLibrary(Core.NATIVE_LIBRARY_NAME);  
        Mat resizedImage = new Mat();  
        Imgproc.resize(originalImage, resizedImage, new Size((double)92,(double)112));  
        return resizedImage;  
    }  
  
    public static String GetResourceFilePath(String filename) {  
  
        InputStream inputStream = null;  
        OutputStream outputStream = null;  
        String tempFilename = "/tmp" + filename;  
        try {  
            // read this file into InputStream  
            inputStream = App.class.getResourceAsStream(filename);  
            if (inputStream == null)  
                System.out.println("empty streaming");  
            // write the inputStream to a FileOutputStream  
            outputStream =  
                    new FileOutputStream(tempFilename);  
  
            int read;  
            byte[] bytes = new byte[102400];  
  
            while ((read = inputStream.read(bytes)) != -1) {  
                outputStream.write(bytes, 0, read);  
//                System.out.println("read bytes is " + Integer.toString(read));  
            }  
            outputStream.flush();  
  
            System.out.println("Load XML file, Done!");  
  
        } catch (IOException e) {  
            e.printStackTrace();  
        } finally {  
            if (inputStream != null) {  
                try {  
                    inputStream.close();  
                } catch (IOException e) {  
                    e.printStackTrace();  
                }  
            }  
            if (outputStream != null) {  
                try {  
                    // outputStream.flush();  
                    outputStream.close();  
                } catch (IOException e) {  
                    e.printStackTrace();  
                }  
  
            }  
  
        }  
        return tempFilename;  
    }  
}  