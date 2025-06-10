package serviceb.misc;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.arrow.ArrowConversion;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.joda.time.Instant;

import serviceb.arrowio.Stream;
import serviceb.arrowio.arrowIO;

public class Demo4{
    
    public static void main(String[] args){
        final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        
        // Orders schema: order_id, customer_id, amount, event_time
        final List<Field> ordersFields = List.of(
            new Field("order_id", FieldType.nullable(new ArrowType.Int(32, true)), null),
            new Field("customer_id", FieldType.nullable(new ArrowType.Int(32, true)), null),
            new Field("amount", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null),
            new Field("event_time", FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC")), null) // timestamp as long
        );
        final Schema ordersSchema = new Schema(ordersFields, Map.of());
        
        // Customers schema: customer_id, name
        final List<Field> customersFields = List.of(
            new Field("customer_id", FieldType.nullable(new ArrowType.Int(32, true)), null),
            new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)
        );
        final Schema customersSchema = new Schema(customersFields, Map.of());
        
        // Create streams
        Stream ordersStream = new Stream("OrdersTopic", ordersSchema, 10,new LinkedBlockingQueue<>());
        Stream customersStream = new Stream("CustomersTopic", customersSchema, 10,new LinkedBlockingQueue<>());
        
        try{
            arrowIO.Registry.createContext("TestSession");
            arrowIO.Registry.AddStream("TestSession", "OrdersStream", ordersStream);
        }catch(Exception e){
            e.printStackTrace(System.out);
        }
        
        Demo4 demo = new Demo4();
        try{
            Stream testOrdersStream = arrowIO.Registry.getStream("TestSession", "OrdersStream");
            
            demo.startOrdersFakerThread(testOrdersStream.streamSchema, allocator, testOrdersStream.getQueue());
        }catch(Exception e){ 
            e.printStackTrace();
        }
        
        org.apache.beam.sdk.schemas.Schema ordersBeamSchema = ArrowConversion.ArrowSchemaTranslator.toBeamSchema(ordersSchema.getFields());
        
        DirectOptions directRunnerOptions = PipelineOptionsFactory.as(DirectOptions.class);
        directRunnerOptions.setTargetParallelism(1);
        Pipeline p = Pipeline.create(directRunnerOptions);

        PCollection<Row> orders = p.apply("readOrders", Read.from(new arrowIO("OrdersTopic", "OrdersStream", "TestSession", Instant.now())))
         .setRowSchema(ordersBeamSchema);
        
        
        PCollectionTuple collection=PCollectionTuple.of(new TupleTag<>("orders"),orders);

        //PCollection<Row> output=collection.apply(SqlTransform.query("SELECT * FROM orders WHERE amount>500"));
        /*PCollection<Row> result = collection.apply(SqlTransform.query(
            "SELECT " +
            "  o.order_id, " +
            "  c.name, " +
            "  o.amount, " +
            "  TUMBLE_START(o.event_time, INTERVAL '10' SECOND) AS window_start " +
            "FROM orders o " +
            "JOIN customers c " +
            "  ON o.customer_id = c.customer_id " +
            "GROUP BY TUMBLE(o.event_time, INTERVAL '10' SECOND), o.order_id, c.name, o.amount"
        ));*/

    PCollection<Row> result = collection
    .apply(SqlTransform.query(
        "SELECT COUNT(*) AS order_count, " +
        "TUMBLE_START(event_time, INTERVAL '10' SECOND) AS window_start " +
        "FROM orders " +
        "GROUP BY TUMBLE(event_time, INTERVAL '10' SECOND)"
    ));
        org.apache.beam.sdk.schemas.Schema outputSchema=result.getSchema();

        System.out.println(outputSchema);
        result.apply("PrintRows", MapElements
            .into(TypeDescriptor.of(Row.class))
            .via((Row row) -> {
                System.out.println("ROW -> " + row);
                return row;
            })
            ).setRowSchema(outputSchema);
        
        p.run().waitUntilFinish();
    }

    public class OrdersFaker implements Runnable {
        private final Schema schema;
        private final BufferAllocator allocator;
        private final BlockingQueue<VectorSchemaRoot> queue;

        public OrdersFaker(Schema schema, BufferAllocator allocator, BlockingQueue<VectorSchemaRoot> queue) {
            this.schema = schema;
            this.allocator = allocator;
            this.queue = queue;
        }

        private VectorSchemaRoot createOrdersTable(Schema schema, BufferAllocator allocator) {
            System.out.println("Table created");
            Map<String, String> metadata = Map.of(
                    "timestamp", Long.toString(Instant.now().getMillis())
            );
            Schema schemaWithMetadata = new Schema(schema.getFields(), metadata);

            VectorSchemaRoot root = VectorSchemaRoot.create(schemaWithMetadata, allocator);
            root.allocateNew();

            IntVector orderId = (IntVector) root.getVector("order_id");
            IntVector customerId = (IntVector) root.getVector("customer_id");
            Float8Vector amount = (Float8Vector) root.getVector("amount");
            TimeStampVector eventTime = (TimeStampVector) root.getVector("event_time");

            int orderIdValue = ThreadLocalRandom.current().nextInt(1000, 9999);
            int customerIdValue = ThreadLocalRandom.current().nextInt(1, 101); // 1-100 customers
            double amountValue = ThreadLocalRandom.current().nextDouble(10.0, 1000.0);
            long TimestampValue= Instant.now().getMillis();

            orderId.setSafe(0, orderIdValue);
            customerId.setSafe(0, customerIdValue);
            amount.setSafe(0, amountValue);
            eventTime.setSafe(0,TimestampValue);

            root.setRowCount(1);
            return root;
        }

        @Override
        public void run() {
            while (true) {
                try {
                    VectorSchemaRoot table = createOrdersTable(schema, allocator);
                    queue.put(table);
                    Thread.sleep(1000); // emit every 1 second
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    public class CustomersFaker implements Runnable {
        private final Schema schema;
        private final BufferAllocator allocator;
        private final BlockingQueue<VectorSchemaRoot> queue;
        private final String[] customerNames = {
            "Alice Johnson", "Bob Smith", "Charlie Brown", "Diana Prince", "Eve Wilson",
            "Frank Miller", "Grace Lee", "Henry Davis", "Ivy Chen", "Jack Wilson"
        };

        public CustomersFaker(Schema schema, BufferAllocator allocator, BlockingQueue<VectorSchemaRoot> queue) {
            this.schema = schema;
            this.allocator = allocator;
            this.queue = queue;
        }

        private VectorSchemaRoot createCustomersTable(Schema schema, BufferAllocator allocator) {
            Map<String, String> metadata = Map.of(
                    "timestamp", Long.toString(Instant.now().getMillis())
            );
            Schema schemaWithMetadata = new Schema(schema.getFields(), metadata);

            VectorSchemaRoot root = VectorSchemaRoot.create(schemaWithMetadata, allocator);
            root.allocateNew();

            IntVector customerId = (IntVector) root.getVector("customer_id");
            VarCharVector name = (VarCharVector) root.getVector("name");

            int customerIdValue = ThreadLocalRandom.current().nextInt(1, 101);
            String nameValue = customerNames[ThreadLocalRandom.current().nextInt(customerNames.length)];

            customerId.setSafe(0, customerIdValue);
            name.setSafe(0, new Text(nameValue).getBytes());

            root.setRowCount(1);
            return root;
        }

        @Override
        public void run() {
            while (true) {
                try {
                    VectorSchemaRoot table = createCustomersTable(schema, allocator);
                    queue.put(table);
                    Thread.sleep(2000); // emit every 2 seconds (slower than orders)
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    public void startOrdersFakerThread(Schema arrowSchema, BufferAllocator allocator, BlockingQueue<VectorSchemaRoot> queue) {
        System.out.println(ArrowConversion.ArrowSchemaTranslator.toBeamSchema(arrowSchema.getFields()));
        Thread t = new Thread(new OrdersFaker(arrowSchema, allocator, queue));
        t.start();
    }

    public void startCustomersFakerThread(Schema arrowSchema, BufferAllocator allocator, BlockingQueue<VectorSchemaRoot> queue) {
        Thread t = new Thread(new CustomersFaker(arrowSchema, allocator, queue));
        t.start();
    }
} 
