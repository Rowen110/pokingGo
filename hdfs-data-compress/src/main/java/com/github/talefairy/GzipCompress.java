package com.github.talefairy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

/**
 * Created by fc.w on 2017/7/14.
 */
public class GzipCompress {

    private static final Logger logger = LoggerFactory.getLogger(GzipCompress.class);

    /**
     * 压缩文件
     * @param srcFile 要被压缩的文件路径
     * @param destFile 压缩后的文件路径
     * @param codecClassName
     * @throws Exception
     */
    public static void compress(String srcFile, String destFile, String codecClassName) throws Exception{
        System.out.println("start access.......");

        Class<?> codecClass = Class.forName(codecClassName);
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        CompressionCodec codec = (CompressionCodec)ReflectionUtils.newInstance(codecClass, conf);

        for (int i = 297; i >= 198; i--) {
            String dateFile = getNAgoday(i, "yyyy-MM-dd");
            System.out.println(dateFile + "  start compress  .......");
            // 拼接文件目录路径
            String newSrcFile = srcFile + "/" + dateFile;
            String newDestFile = destFile + "/" + dateFile;
            System.out.println("压缩文件源路径：" + newSrcFile);
            System.out.println("压缩文件存放路径：" + newDestFile);
            Path srcPath = new Path(newSrcFile);
            FileSystem fs = FileSystem.get(conf);
            List<String> files = getFile(srcPath, fs);
            if (! files.isEmpty()) {
                for (int k = 0; k < files.size(); k++) {
                    String pathFile = files.get(k);
                    System.out.println("文件名称：" + pathFile);
                    //指定压缩文件路径
                    FSDataOutputStream outputStream = fs.create(new Path(newDestFile));
                    //指定要被压缩的文件路径
                    FSDataInputStream in = fs.open(new Path(pathFile));
                    CompressionOutputStream out = codec.createOutputStream(outputStream);
                    IOUtils.copyBytes(in, out, conf);
                    IOUtils.closeStream(in);
                    IOUtils.closeStream(out);
                    System.out.println(dateFile + "  success .......");
                }
            }

        }

    }

    /**
     * 获取文件
     * @param path
     * @param fs
     * @throws IOException
     */
    public static List<String> getFile(Path path, FileSystem fs) throws IOException {
        List<String> paths = new ArrayList<String>();
        //获取目录下所有子文件或子文件夹
        FileStatus[] fileStatus = fs.listStatus(path);
        for (int i = 0; i < fileStatus.length; i++) {
            // 如果是子目录则继续迭代
            if (fileStatus[i].isDirectory()) {
                Path p = new Path(fileStatus[i].getPath().toString());
                getFile(p,fs);
            } else {
                // 获取文件名路径
                paths.add(fileStatus[i].getPath().toString());
            }
        }

        return paths;
    }


    /**
     * 获取前N天时间
     * @param n
     * @return
     */
    public static String getNAgoday(int n, String format) {
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE, -n);
        SimpleDateFormat dateFormat = new SimpleDateFormat(format);
        String dayStr = dateFormat.format(cal.getTime());
        return dayStr;
    }

    /**
     * 解压缩
     * @param compressFile 压缩文件路径
     * @param decompressFile 解压文件路径
     * @param codecClassName org.apache.hadoop.io.compress.GzipCodec
     * @throws Exception
     */
    public static void uncompress(String compressFile, String decompressFile , String codecClassName) throws Exception{
        Class<?> codecClass = Class.forName(codecClassName);
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        FileSystem fs = FileSystem.get(conf);
        CompressionCodec codec = (CompressionCodec)ReflectionUtils.newInstance(codecClass, conf);
        FSDataInputStream inputStream = fs.open(new Path(compressFile));
        //把text文件里到数据解压，然后输出到控制台
        InputStream in = codec.createInputStream(inputStream);
        IOUtils.copyBytes(in, System.out, conf);
        IOUtils.closeStream(in);
    }

    /**
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        // 压缩
        if (args[0].equals("compress")) {
            compress(args[1], args[2], "org.apache.hadoop.io.compress." + args[3]);
        } else if (args[0].equals("decompress")) // 解压
            uncompress(args[1], args[2], "org.apache.hadoop.io.compress." + args[3]);
        else {
            System.err.println("Error!\n usgae: hadoop jar Hello.jar [compress] [srcFileName] [descFileName] [compress type]");
            System.err.println("\t\ror [decompress] [filename] ");
            return;
        }

        System.out.println("down");
    }

}
