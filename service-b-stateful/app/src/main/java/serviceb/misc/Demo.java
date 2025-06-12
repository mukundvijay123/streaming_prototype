package serviceb.misc;


//This is the demo for the IO class 

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.arrow.ArrowConversion;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.joda.time.DateTime;
import org.joda.time.Instant;

import serviceb.arrowio.Stream;
import serviceb.arrowio.arrowIO;


public class Demo {
    
    public static void main(String[] args){
        final BlockingQueue<VectorSchemaRoot> queue = new LinkedBlockingQueue<>();
        final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        final List<Field> fields = List.of(
            new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
            new Field("name", FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("score", FieldType.nullable(new ArrowType.Int(32, true)), null)
        );
        final Schema arrowSchema = new Schema(fields, Map.of());
        Stream stream =new Stream("MyTopic",arrowSchema,10,new LinkedBlockingQueue<>());
        try{
            arrowIO.Registry.createContext("TestSession");
            arrowIO.Registry.AddStream("TestSession","SomeStream", stream);
        }catch(Exception e){
            e.printStackTrace(System.out);
        }
        Demo demo = new Demo();
        try{
            
            Stream testStream=arrowIO.Registry.getStream("TestSession", "SomeStream");
            demo.startFakerThread(testStream.streamSchema,allocator,testStream.getQueue());
        }catch(Exception e){ 
            e.printStackTrace();
        }
        org.apache.beam.sdk.schemas.Schema beamSchema = ArrowConversion.ArrowSchemaTranslator.toBeamSchema(arrowSchema.getFields());
        DirectOptions directRunnerOptions =PipelineOptionsFactory.as(DirectOptions.class);
        directRunnerOptions.setTargetParallelism(1);
        Pipeline p = Pipeline.create(directRunnerOptions);

        PCollection<Row> input=p.apply("readFromQueue",Read.from(new arrowIO("someTopic","SomeStream","TestSession", Instant.now())))
         .setRowSchema(beamSchema);
        
        input.apply("PrintRows", MapElements
            .into(TypeDescriptor.of(Row.class))
            .via((Row row) -> {
                System.out.println("ROW -> " + row);
                return row;
            })
        ).setRowSchema(beamSchema);
        p.run().waitUntilFinish();


    }





    public class Faker implements Runnable {
        private final Schema schema;
        private final BufferAllocator allocator;
        private final BlockingQueue<VectorSchemaRoot> queue;

        public Faker(Schema schema, BufferAllocator allocator, BlockingQueue<VectorSchemaRoot> queue) {
            this.schema = schema;
            this.allocator = allocator;
            this.queue = queue;
        }

        private VectorSchemaRoot createTable(Schema schema, BufferAllocator allocator) {
            Map<String, String> metadata = Map.of(
                    "timestamp", Long.toString(DateTime.now().getMillis())
            );
            Schema schemaWithMetadata = new Schema(schema.getFields(), metadata);

            VectorSchemaRoot root = VectorSchemaRoot.create(schemaWithMetadata, allocator);
            root.allocateNew();

            IntVector id = (IntVector) root.getVector("id");
            VarCharVector name = (VarCharVector) root.getVector("name");
            IntVector score = (IntVector) root.getVector("score");

            id.setSafe(0, ThreadLocalRandom.current().nextInt(0, 100));
            name.setSafe(0, new Text("name_0").getBytes());
            score.setSafe(0, ThreadLocalRandom.current().nextInt(0, 100));

            root.setRowCount(1);
            return root;
        }

        @Override
        public void run() {
            
            while (true) {
                
                try {
                    VectorSchemaRoot table = createTable(schema, allocator);
                    queue.put(table);
                    Thread.sleep(1000); // emit every 1 second
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    public void startFakerThread(Schema arrowSchema,BufferAllocator allocator ,BlockingQueue<VectorSchemaRoot> queue) {
        Thread t = new Thread(new Faker(arrowSchema, allocator, queue));
        t.start();
    }
}
