package test;

import com.google.common.collect.ImmutableList;
import main.trait.Optimizer;
import org.apache.calcite.DataContexts;
import org.apache.calcite.adapter.enumerable.EnumerableTableScan;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.materialize.MaterializationService;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rex.RexExecutorImpl;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.List;

import static org.apache.calcite.tools.Frameworks.newConfigBuilder;

public class TestSubvisitor {
    public static void main(String[] args) {
//        String query = "select \"empid\" + 1 from \"emps\" where \"deptno\" = 10";
//        final ImmutableList<Pair<String, String>> materializationList = ImmutableList.of(Pair.of("select * from \"emps\" where \"deptno\" = 10", "MV0"));
//        FrameworkConfig config = newConfigBuilder() //
//                .defaultSchema(Frameworks.createRootSchema(true)).build();
//        System.out.println(config.getDefaultSchema());
//        RelOptMaterialization relOptMaterialization = new RelOptMaterialization();
        String testSql = "select l_discount,count (distinct l_orderkey), sum(distinct l_tax)\n" +
                "from lineitem, part\n" +
                "where l_discount > 100 group by l_discount;";

        testSql = testSql.replace(";", "");
//        RelNode testRelNode = new Optimizer().parse(testSql);
//        HepPlanner planner = new HepPlanner(HepProgram.builder().build());
//        RelNode tableRel = sql("select * from dept").toRel();
//        RelNode queryRel = tableRel;
//        RelOptMaterialization mat1 =
//                new RelOptMaterialization(tableRel, queryRel, null,
//                        ImmutableList.of("default", "mv"));
//        planner.addMaterialization(mat1);
//        assertThat(planner.getMaterializations(), hasSize(1));
    }

//    final SchemaPlus defaultSchema;
//    Frameworks.withPlanner((cluster,relOptSchema,rootSchema)->
//    {
//        cluster.getPlanner().setExecutor(new RexExecutorImpl(DataContexts.EMPTY));
//        try {
//            final SchemaPlus defaultSchema;
//            if (f.schemaSpec == null) {
//                defaultSchema =
//                        rootSchema.add("hr",
//                                new ReflectiveSchemaWithoutRowCount(new MaterializationTest.HrFKUKSchema()));
//            } else {
//                defaultSchema = CalciteAssert.addSchema(rootSchema, f.schemaSpec);
//            }
//            final RelNode queryRel = toRel(cluster, rootSchema, defaultSchema, f.query);
//            final List<RelOptMaterialization> mvs = new ArrayList<>();
//            final RelBuilder relBuilder =
//                    RelFactories.LOGICAL_BUILDER.create(cluster, relOptSchema);
//            final MaterializationService.DefaultTableFactory tableFactory =
//                    new MaterializationService.DefaultTableFactory();
//            for (Pair<String, String> pair : f.materializationList) {
//                String sql = pair.left;
//                final RelNode mvRel = toRel(cluster, rootSchema, defaultSchema, sql);
//                final Table table =
//                        tableFactory.createTable(CalciteSchema.from(rootSchema),
//                                sql, ImmutableList.of(defaultSchema.getName()));
//                String name = pair.right;
//                defaultSchema.add(name, table);
//                relBuilder.scan(defaultSchema.getName(), name);
//                final LogicalTableScan logicalScan = (LogicalTableScan) relBuilder.build();
//                final EnumerableTableScan replacement =
//                        EnumerableTableScan.create(cluster, logicalScan.getTable());
//                mvs.add(
//                        new RelOptMaterialization(replacement, mvRel, null,
//                                ImmutableList.of(defaultSchema.getName(), name)));
//            }
//            return new TestConfig(defaultSchema.getName(), queryRel, mvs);
//        } catch (Exception e) {
//            throw TestUtil.rethrow(e);
//        }
//    });
}
