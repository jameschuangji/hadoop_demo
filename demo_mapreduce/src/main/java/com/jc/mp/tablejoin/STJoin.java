package com.jc.mp.tablejoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Iterator;


public class STJoin {
    public static int time = 0;

    //Map将输入分割成child 和parent，然后正序输出一次作为右表，反许输出一次作为左表，需要注意
    //的是在输出的value中必须加上左右表区别标志
    public static class Map extends Mapper<Object, Text,Text, Text>{
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String childName = new String();
            String parentName = new String();
            String relationType = new String();
            String line = value.toString();
            int i = 0;
            while (line.charAt(i) != ' '){
                i++;
            }

            String[] values = {line.substring(0,i),line.substring(i+1)};
            if (values[0].compareTo("child")!= 0){
                childName = values[0];
                parentName = values[1];
                relationType = "1"; //左右表区分标志
                context.write(new Text(values[1]),new Text(relationType + "+" + childName + "+" + parentName)); //左表
                relationType ="2";
                context.write(new Text(values[0]),new Text(relationType + "+" + childName + "+" + parentName)); //右表
            }
        }
    }

    public static class Reduce extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            if (time == 0){
                //输出表头
                context.write(new Text("grandchild"),new Text("grandparent"));
                time++;
            }
            int grandchildnum = 0;
            String grandchild[] = new String[10];
            int grandparentnum = 0;
            String grandparent[] = new String[10];
            Iterator iter= values.iterator();
            while (iter.hasNext()){
                String record = iter.next().toString();
                int len = record.length();
                int i = 2;
                if (len == 0) continue;
                char relationType = record.charAt(0);
                String childname = new String();
                String parentname = new String();

                //获取vlaue-list中的value的child
                while (record.charAt(i) != '+'){
                    childname = childname + record.charAt(i);
                    i++;
                }
                i = i + 1;
                //获取vlaue-list中的value的parent
                while (i < len){
                    parentname = parentname + record.charAt(i);
                    i++;
                }

                //左表，取出child放入grandchild
                if (relationType == '1'){
                    grandchild[grandchildnum] = childname;
                    grandchildnum++;
                }else {
                    //右表，取出parent放入grandparent
                    grandparent[grandparentnum]=parentname;
                    grandparentnum++;
                }

                //grandchild grandparent数组求笛卡儿积
                if (grandparentnum != 0 && grandchildnum != 0){
                    for (int m = 0; m<grandchildnum;m++){
                        for (int n = 0; n<grandparentnum;n++){
                            context.write(new Text(grandchild[m]),new Text(grandparent[n]));
                        }
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
        if (otherArgs.length !=2){
            System.err.println("Usage: STJoin <in> <out>");
            System.exit(2);
        }

        Job job= new Job(conf,"single table join");
        job.setJarByClass(STJoin.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job,new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job,new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true)? 0:1);
    }
}
