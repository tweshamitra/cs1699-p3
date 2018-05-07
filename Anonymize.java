
import org.deidentifier.arx.*;
import org.deidentifier.arx.aggregates.StatisticsSummary;
import org.deidentifier.arx.criteria.KAnonymity;
import java.util.*;
import java.lang.Long;
import org.deidentifier.arx.DataType;
import org.deidentifier.arx.ARXLattice.ARXNode;
import org.deidentifier.arx.DataHandle;
import org.deidentifier.arx.AttributeType.Hierarchy;
import org.deidentifier.arx.criteria.HierarchicalDistanceTCloseness;
import org.deidentifier.arx.criteria.EqualDistanceTCloseness;
import org.deidentifier.arx.AttributeType.Hierarchy.DefaultHierarchy;
import org.deidentifier.arx.aggregates.HierarchyBuilderGroupingBased.Level;
import org.deidentifier.arx.aggregates.HierarchyBuilder;
import org.deidentifier.arx.aggregates.HierarchyBuilder.Type;
import org.deidentifier.arx.aggregates.HierarchyBuilderIntervalBased;
import org.deidentifier.arx.aggregates.HierarchyBuilderIntervalBased.Interval;
import org.deidentifier.arx.aggregates.HierarchyBuilderIntervalBased.Range;
import org.deidentifier.arx.aggregates.HierarchyBuilderRedactionBased;
import org.deidentifier.arx.aggregates.HierarchyBuilderRedactionBased.Order;
import org.deidentifier.arx.aggregates.StatisticsFrequencyDistribution;
import org.deidentifier.arx.criteria.DistinctLDiversity;

import org.apache.commons.math3.distribution.LaplaceDistribution;
@SuppressWarnings("unchecked")
public class Anonymize{
    static String filename = "taxData.xls";
    static String filename_modified = "taxData_mod.xls";
    static boolean l_closeness = false;
    public static void main(String[] args){
        Scanner s = new Scanner(System.in);
        int choice = 0;
        Data data = null;
        ARXResult result = null;
        boolean flag = true;
        while(flag){
        System.out.println("\nChoose an option below(-1 to quit):\n1.Extract Data\n2.Anonymize Data\n3.Check Optimum Generization Levels\n4.Apply Laplace Mechanism\n");
        choice = s.nextInt();
            switch(choice){
                case 1:
                    diff(filename, filename_modified, false);
                    break;
                case 2:
                    if(data==null){
                        data = initializeData(filename);
                    }
                    result = transform(data);
                    break;
                case 3:
                    if(data==null){
                        data = initializeData(filename);
                        result = transform(data);
                    }
                    getInsights(result, data);
                    break;
                case 4:
                    diff(filename, filename_modified, true);
                    break;
                case 5:
                    l_closeness = true;
                    break;
                case -1:
                    flag = false;
                    break;
            }
        }
    }
    public static Data initializeData(String filename){
        Data data = null;
        DataSource source = null;
        try{
            source = DataSource.createExcelSource(filename, 0, true);
        }catch(Exception e){
            e.printStackTrace();
        }
        source.addColumn("ein", DataType.INTEGER);
        source.addColumn("zip", DataType.STRING);
        source.addColumn("tax_year", DataType.STRING);
        source.addColumn("revenue_amt", DataType.STRING);

        try{
            data = Data.create(source);
        }catch(Exception e2){
            e2.printStackTrace();
        }
        return data;
    }

    public static int extract(String filename){
        Long total = 0L;
        int count_for_avg=0;
        Data data = null;
        DataSource source = null;
        try{
            source = DataSource.createExcelSource(filename, 0, true);
            source.addColumn("revenue_amt", DataType.STRING);
            data = Data.create(source);
        } catch (Exception e){
            e.printStackTrace();
        }
       
        Iterator<String[]> iterator = data.getHandle().iterator();
        int count = 0;
        Long target= 10000000L;
        Long current = 0L;
        String curr, check = "";
        while(iterator.hasNext()){
            curr = Arrays.toString(iterator.next());
            if(curr.equals("[revenue_amt]")){
                continue;
            }
            else if(curr.equals("[]")){
                continue;
            }
            else{
                int left,right;
                left = curr.indexOf("[");
                right = curr.indexOf("]");
                check = curr.substring(left+1, right);
                current = Long.parseLong(check);
                if(current!=0L){
                    total+=current;
                    count_for_avg++;
                }
                if(current > target){
                    count++;
                }
            }
        }
        return count;
   }
    public static void diff(String data, String modified, boolean laplace){
        int original_count = extract(data);
        double epsilon = 0.1;
        if(laplace){
            int modified_count = extract(modified);
            System.out.println("D:"+ original_count+" Non-profit organizations filed revenue of >$10,000,000 on Form 990 between 1985-2017.\n");
            System.out.println("D' (before Laplacian Noise):  " + modified_count+" Non-profit organizations filed revenue as >$10,000,000 on Form 990 between 1985-2017.\n");
            LaplaceDistribution _ld = new LaplaceDistribution(0, 1/epsilon);
            double noise = _ld.sample();
            double result = noise + modified_count;
            System.out.println("D' (after Laplacian Noise):  " + (int)result+" Non-profit organizations filed revenue of >$10,000,000 on Form 990 between 1985-2017.\n");

        }
    }
   
    public static Map<String, StatisticsSummary<?>> getStatistics(Data data){
        Map<String,StatisticsSummary<?>> toReturn = data.getHandle().getStatistics().getSummaryStatistics(true);
        return toReturn;
    }
    public static ARXResult transform(Data data){
      
        HierarchyBuilderRedactionBased builder2 = HierarchyBuilderRedactionBased.create(Order.LEFT_TO_RIGHT, Order.RIGHT_TO_LEFT, ' ', '*');
        data.getDefinition().setAttributeType("ein", AttributeType.QUASI_IDENTIFYING_ATTRIBUTE);
        data.getDefinition().setAttributeType("zip", AttributeType.QUASI_IDENTIFYING_ATTRIBUTE);
        data.getDefinition().setAttributeType("tax_year", AttributeType.QUASI_IDENTIFYING_ATTRIBUTE);
        data.getDefinition().setAttributeType("revenue_amt", AttributeType.QUASI_IDENTIFYING_ATTRIBUTE);
        data.getDefinition().setHierarchy("ein", builder2);
        data.getDefinition().setHierarchy("zip", builder2);
        data.getDefinition().setHierarchy("revenue_amt",builder2);
        data.getDefinition().setAttributeType("tax_year", builder2);
        data.getDefinition().setMaximumGeneralization("revenue_amt", 5);

        ARXAnonymizer anonymizer = new ARXAnonymizer();
        ARXResult result = null;
        ARXConfiguration config = ARXConfiguration.create();
      
        config.addPrivacyModel(new KAnonymity(2));
        config.setSuppressionLimit(1d);
        
        try{
            result = anonymizer.anonymize(data, config);
            result.getOutput(true).save("transformed_dataset.csv",';');
        } catch (Exception e){
            e.printStackTrace();
        }
        System.out.println("Check out transformed_dataset.csv to see the anonymized dataset");
        return result;
    }

    public static void getInsights(ARXResult result, Data data){
        DataHandle handle = result.getOutput();
        Map<String,StatisticsSummary<?>> statistics = data.getHandle().getStatistics().getSummaryStatistics(true);
        StatisticsSummary<String> revenueStatistics = (StatisticsSummary<String>)statistics.get("revenue_amt");
        ARXNode optimum = result.getGlobalOptimum();
        List<String> quasi_id = new ArrayList<String>(data.getDefinition().getQuasiIdentifyingAttributes());
        StringBuffer[] identifiers = new StringBuffer[quasi_id.size()];
        StringBuffer[] generalizations = new StringBuffer[quasi_id.size()];
        int lengthI = 0;
        int lengthG = 0;
        for (int i = 0; i < quasi_id.size(); i++) {
            identifiers[i] = new StringBuffer();
            generalizations[i] = new StringBuffer();
            identifiers[i].append(quasi_id.get(i));
            generalizations[i].append(optimum.getGeneralization(quasi_id.get(i)));
            if (data.getDefinition().isHierarchyAvailable(quasi_id.get(i)))
                generalizations[i].append("/").append(data.getDefinition().getHierarchy(quasi_id.get(i))[0].length - 1);
            lengthI = Math.max(lengthI, identifiers[i].length());
            lengthG = Math.max(lengthG, generalizations[i].length());
        }

        for (int i = 0; i < quasi_id.size(); i++) {
            while (identifiers[i].length() < lengthI) {
                identifiers[i].append(" ");
            }
            while (generalizations[i].length() < lengthG) {
                generalizations[i].insert(0, " ");
            }
        }

        System.out.println("Generalization levels:\n");
        for (int i = 0; i < quasi_id.size(); i++) {
            System.out.println(identifiers[i] + ": " + generalizations[i]);
        }

    }
    
}