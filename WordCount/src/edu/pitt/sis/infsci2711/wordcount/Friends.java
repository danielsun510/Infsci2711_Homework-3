package edu.pitt.sis.infsci2711.wordcount;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;





public class Friends extends Configured implements Tool {
   public static void main(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      int res = ToolRunner.run(new Configuration(), new Friends(), args);
      
      System.exit(res);
   }

   @Override
   public int run(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      Job job = new Job(getConf(), "Friends");
      job.setJarByClass(WordCount.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);

      job.setMapperClass(Map.class);
      job.setReducerClass(Reduce.class);

      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);

      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      job.waitForCompletion(true);
      
      return 0;
   }
   
   static public class FriendsWritable implements Writable {
	    public Long user;
	    public Long mutualFriend;

	    public FriendsWritable(Long user, Long mutualFriend) {
	        this.user = user;
	        this.mutualFriend = mutualFriend;
	    }

	    public FriendsWritable() {
	        this(-1L, -1L);
	    }

	    @Override
	    public void write(DataOutput out) throws IOException {
	        out.writeLong(user);
	        out.writeLong(mutualFriend);
	    }

	    @Override
	    public void readFields(DataInput in) throws IOException {
	        user = in.readLong();
	        mutualFriend = in.readLong();
	    }

	}

   
   
   public static class Map extends Mapper<LongWritable, Text, LongWritable, FriendsWritable> {
	    private Text word = new Text();

	    @Override
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        String line[] = value.toString().split("\t");
	        Long fromUser = Long.parseLong(line[0]);
	        List toUsers = new ArrayList();

	        if (line.length == 2) {
	            StringTokenizer tokenizer = new StringTokenizer(line[1], ",");
	            while (tokenizer.hasMoreTokens()) {
	                Long toUser = Long.parseLong(tokenizer.nextToken());
	                toUsers.add(toUser);
	                context.write(new LongWritable(fromUser),
	                        new FriendsWritable(toUser, -1L));
	            }

	            for (int i = 0; i < toUsers.size(); i++) {
	                for (int j = i + 1; j < toUsers.size(); j++) {
	                    context.write(new LongWritable((long)toUsers.get(i)),
	                            new FriendsWritable((long)(toUsers.get(j)), fromUser));
	                    context.write(new LongWritable((long)toUsers.get(j)),
	                            new FriendsWritable((long)(toUsers.get(i)), fromUser));
	                }
	                }
	            }
	        }
	    }

   public static class Reduce extends Reducer<LongWritable, FriendsWritable, LongWritable, Text> {
	    @Override
	    public void reduce(LongWritable key, Iterable values, Context context)
	            throws IOException, InterruptedException {

	        // key is the recommended friend, and value is the list of mutual friends
	        final java.util.Map<Long, List<Long>> mutualFriends = new HashMap<Long, List<Long>>();

	        for (FriendsWritable val : values) {
	            final Boolean isAlreadyFriend = (val.mutualFriend == -1);
	            final Long toUser = val.user;
	            final Long mutualFriend = val.mutualFriend;

	            if (mutualFriends.containsKey(toUser)) {
	                if (isAlreadyFriend) {
	                    mutualFriends.put(toUser, null);
	                } else if (mutualFriends.get(toUser) != null) {
	                    mutualFriends.get(toUser).add(mutualFriend);
	                }
	            } else {
	                if (!isAlreadyFriend) {
	                    mutualFriends.put(toUser, new ArrayList() {
	                        {
	                            add(mutualFriend);
	                        }
	                    });
	                } else {
	                    mutualFriends.put(toUser, null);
	                }
	            }
	        }

	        java.util.SortedMap<Long, List> sortedMutualFriends = new TreeMap<Long, List>(new Comparator() {
	            public int compare(Long key1, Long key2) {
	                Integer v1 = mutualFriends.get(key1).size();
	                Integer v2 = mutualFriends.get(key2).size();
	                if (v1 > v2) {
	                    return -1;
	                } else if (v1.equals(v2) && key1 < key2) {
	                    return -1;
	                } else {
	                    return 1;
	                }
	            }

	        });

	        for (java.util.Map.Entry<Long, List<Long>> entry : mutualFriends.entrySet()) {
	            if (entry.getValue() != null) {
	                sortedMutualFriends.put(entry.getKey(), entry.getValue());
	            }
	        }

	        String FriendRecomList="";
			for(Entry<Long,List<Long>> entry:list){
				FriendRecomList+=entry.getKey()+",";
			}
			if(list.size()>1){
				FriendRecomList.substring(0,FriendRecomList.length()-1);
			}
	    	context.write(key, new Text(FriendRecomList));
	      }
	    
	}
}

   
   

