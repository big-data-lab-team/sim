import niftijio.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.StringTokenizer;
import java.util.Arrays;
import java.util.HashMap;
import java.io.IOException;
import java.io.PrintWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

public class GetHistogram {

    public static class VoxelValueMapper extends Mapper<Object, Text, DoubleWritable, IntWritable> {
        

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException, ClassCastException {
                
            HashMap<Double, Integer> histogram = new HashMap<Double, Integer>();

            //read nii.tar.gz image located at filepath
            NiftiVolume volume = NiftiVolume.read(value.toString());

            int nx = volume.header.dim[1];
            int ny = volume.header.dim[2];
            int nz = volume.header.dim[3];
            int dim = volume.header.dim[4];
            

            double voxelValue;

            //iterate through all voxels in image and check if voxel value already exists. If so, increment count of voxel value in hashtable by 1. Otherwise, add voxel value to hash
            //table with initial count of 1 
            for(int d = 0; d < dim; d++){
                for(int k = 0; k < nz; k++){
                    for (int j = 0; j < ny; j++){
                        for (int i = 0; i < nx; i++){
                            
                            voxelValue = (double) volume.data.get(i, j, k, d);                                

                            if(histogram.containsKey(voxelValue)){

                                    int count = histogram.get(voxelValue) + 1;
                                    histogram.replace(voxelValue, count);
                            }
                            else{
                                    histogram.put(voxelValue, 1);
                            }
                        }
                    }   
                }
            }

            //iterate through all keys (voxel values) in hashtable and write to context key and respective count (value)
            for(double k : histogram.keySet()) {
                    context.write(new DoubleWritable(k) , new IntWritable(histogram.get(k)));
            }
        
        }
    }

    public static class HistogramReducer extends Reducer<DoubleWritable, IntWritable, DoubleWritable, IntWritable>{
        private IntWritable result = new IntWritable();
        public void reduce(DoubleWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            //sum the values to get total count for given voxel value key 
            for (IntWritable val : values){
                count += val.get();
            }
            result.set(count);
            context.write(key, result);
        } 
    }
    public static void main(String[] args){
        try{
            Configuration conf = new Configuration();
            
            GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
            String[] remainingArgs = optionParser.getRemainingArgs();
            if (remainingArgs.length != 2){
                System.err.println("Usage: GetHistogram <in> <out>");
                System.exit(2);
            }
            Job job = Job.getInstance(conf, "histogram");
            job.setJarByClass(GetHistogram.class);
            job.setMapperClass(VoxelValueMapper.class);
            job.setCombinerClass(HistogramReducer.class);
            job.setReducerClass(HistogramReducer.class);
            job.setOutputKeyClass(DoubleWritable.class);
            job.setOutputValueClass(IntWritable.class);  
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            System.exit(job.waitForCompletion(true) ? 0 : 1);           
        }
        catch(ClassCastException e){
            System.err.println("ERROR: It was impossible to cast voxel value to double.");
            System.err.println("Exception - " + e.getMessage());
            System.exit(1);

        }
        catch(Exception e){
            System.err.println("Exception - " + e.getMessage());
            e.printStackTrace(System.err);
        
        }

    }
}
