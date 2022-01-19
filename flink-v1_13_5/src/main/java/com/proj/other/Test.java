package com.proj.other;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.Objects;

public class Test {
    public static void main(String[] args) throws IOException {
//        System.out.println(Arrays.asList("fsad,fdsa,gfsda"));

        Path path = new Path("cp/checkpointDirectory");
        System.out.println(path.getPath());
        FileSystem fileSystem = path.getFileSystem();
//        fileSystem.
//        System.out.println(path.getFileSystem());

        String path1 = Test.class.getResource("/cp").getPath();
//        URL resource = (URL) path1;


//        val inputStream: DataStream[String] = env.readTextFile(resource.getPath)
        File file = new File(path1);
        System.out.println(Arrays.toString(file.listFiles()));

//        Path p1 = file.toPath();
    }
}
