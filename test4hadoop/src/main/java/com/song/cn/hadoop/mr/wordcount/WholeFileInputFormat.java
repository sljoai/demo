package com.song.cn.hadoop.mr.wordcount;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class WholeFileInputFormat extends FileInputFormat<Text, BytesWritable> {
	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public RecordReader<Text, BytesWritable> createRecordReader(InputSplit split,
																TaskAttemptContext context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
//		return null;
		return new SingleFileNameReader((FileSplit) split, context.getConfiguration());
	}
}

/*@SuppressWarnings("rawtypes")
class WholeFileRecordReader extends RecordReader
{
	private FileSplit fileSplit;
	private FSDataInputStream fis;

	private Text key=null;
	private BytesWritable value = null;

	private boolean processed = false;

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return this.key;
	}

	@Override
	public BytesWritable getCurrentValue() throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return this.value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return processed? fileSplit.getLength():0;
	}

	public void initialize(InputSplit inputSplit, TaskAttemptContext tacontext)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		fileSplit = (FileSplit)inputSplit;
		Configuration job = tacontext.getConfiguration();
		Path file = fileSplit.getPath();
		FileSystem fs = file.getFileSystem(job);
		fis = fs.open(file);
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		if(key==null)
		{
			key = new Text();
		}
		if(value==null)
		{
			value = new BytesWritable();
		}
		if(!processed)
		{
			byte[] content = new byte[(int)fileSplit.getLength()];
			Path file = fileSplit.getPath();
			System.out.println(file.getName());
			key.set(file.getName());
			try{
				IOUtils.readFully(fis, content, 0, content.length);
				value.set(new BytesWritable(content));
			}catch(IOException e)
			{
				e.printStackTrace();
			}finally{
				IOUtils.closeStream(fis);
			}
			processed = true;
			return true;//return true表示这次inputformat还没有结束，会有下一对keyvalue产生
		}
		return false;//return false表示这次inputformat结束了
	}
}*/
