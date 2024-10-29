package test;

import com.alibaba.fastjson2.JSONArray;
import com.google.common.collect.ImmutableList;
import main.Rewriter;
import main.Utils;
import org.apache.calcite.config.Lex;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.materialize.MaterializedViewRule;
import org.apache.calcite.rel.rules.materialize.MaterializedViewRules;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.calcite.avatica.util.Casing.UNCHANGED;
import static org.apache.calcite.avatica.util.Quoting.DOUBLE_QUOTE;

public class TestMaterializedView {
    public static HepProgramBuilder builder(RelOptRule rule_instance) {
        HepProgramBuilder builder = new HepProgramBuilder();
//        RelOptRule rule_instance = CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES;
//        builder.addRuleInstance(rule_instance);
//        List<RelOptRule> t = Arrays.asList(CoreRules.AGGREGATE_PROJECT_MERGE, CoreRules.AGGREGATE_VALUES, PruneEmptyRules.AGGREGATE_INSTANCE);
//        List<RelOptRule> t = Arrays.asList(CoreRules.FILTER_TO_CALC, CoreRules.PROJECT_TO_CALC, CoreRules.CALC_MERGE);
        List<RelOptRule> t = Arrays.asList(
                MaterializedViewRules.FILTER_SCAN,
                MaterializedViewRules.PROJECT_AGGREGATE,
                MaterializedViewRules.PROJECT_FILTER,
                MaterializedViewRules.FILTER,
                MaterializedViewRules.JOIN,
                MaterializedViewRules.PROJECT_JOIN,
                MaterializedViewRules.AGGREGATE
        );
//        List<RelOptRule> t = SubstitutionVisitor.DEFAULT_RULES;
        for (RelOptRule rule : t) {
            builder.addRuleInstance(rule);
        }
        return builder;
    }

    //    RelOptPlanner relOptPlanner = new RelOptPlanner().addMaterialization();
//    RelOptMaterializations
//    MaterializedViewSubstitutionVisitor materializedViewSubstitutionVisitor;
    public static void main(String[] args) throws Exception {
        String path = System.getProperty("user.dir");
        JSONArray schemaJson = Utils.readJsonFile(path + "/src/main/java/main/schema.json");
        Rewriter rewriter = new Rewriter(schemaJson);
        String testSql;

        testSql = "select l_discount,count (distinct l_orderkey), sum(distinct l_tax)\n" +
                "from lineitem, part\n" +
                "where l_discount > 100 group by l_discount;";

        testSql = testSql.replace(";", "");
        RelNode testRelNode = rewriter.SQL2RA(testSql);
        RelToSqlConverter converter = new RelToSqlConverter(PostgresqlSqlDialect.DEFAULT);

        RelOptRule rule_instance = MaterializedViewRules.PROJECT_JOIN;

        HepProgramBuilder builder = builder(rule_instance);
        HepPlanner hepPlanner = new HepPlanner(builder.addMatchOrder(HepMatchOrder.TOP_DOWN).build());
        hepPlanner.setRoot(testRelNode);

        String tableSql = "select * from mv0";

        RelNode tableRel = rewriter.SQL2RA(tableSql);
//        RelNode queryRel = tableRel;
        RelOptMaterialization mat1 =
                new RelOptMaterialization(tableRel, testRelNode, null,
                        ImmutableList.of("default", "mv"));
        hepPlanner.addMaterialization(mat1);


//
//        ImmutableList<Pair<String, String>> materializationList = ImmutableList.of(Pair.of(testSql, "MV0"));
//        RelNode testMVRelNode = rewriter.SQL2RA(materializationList.getFirst().left);
//
//        final MaterializationService.DefaultTableFactory tableFactory =
//                new MaterializationService.DefaultTableFactory();
//
//        for (Pair<String, String> pair : materializationList) {
//            String sql = pair.left;
//            final List<RelOptMaterialization> mvs = new ArrayList<>();
//            final RelBuilder relBuilder =
//                    RelFactories.LOGICAL_BUILDER.create(testRelNode.getCluster(), rewriter.rootSchema);
//            final RelNode mvRel = rewriter.SQL2RA(sql);
//            final Table table =
//                    tableFactory.createTable(CalciteSchema.from(rewriter.rootSchema),
//                            sql, ImmutableList.of(rewriter.rootSchema.getName()));
//            String name = pair.right;
//            rewriter.rootSchema.add(name, table);
//            relBuilder.scan(rewriter.rootSchema.getName(), name);
//            final LogicalTableScan logicalScan = (LogicalTableScan) relBuilder.build();
//            final EnumerableTableScan replacement =
//                    EnumerableTableScan.create(testRelNode.getCluster(), logicalScan.getTable());
//            mvs.add(
//                    new RelOptMaterialization(replacement, mvRel, null,
//                            ImmutableList.of(rewriter.rootSchema.getName(), name)));


//
////        List<RelOptMaterialization> relOptMaterializations = RelOptMaterializations.getApplicableMaterializations(testRelNode, List.of(relOptMaterialization));
////        System.out.println(relOptMaterializations.getFirst());
//        final List<RelOptMaterialization> materializations = hepPlanner.getMaterializations();
//        System.out.println(materializations);

//        List<Pair<RelNode, List<RelOptMaterialization>>> relOptMaterializations = RelOptMaterializations.useMaterializedViews(testRelNode, List.of(relOptMaterialization));
//        System.out.println(Objects.requireNonNull(relOptMaterializations.getFirst().left).explain());
//        System.out.println(Objects.requireNonNull(relOptMaterializations.getFirst().right).getFirst().toString());
//        System.out.println(Objects.requireNonNull(relOptMaterializations.getFirst().right).getFirst().toString());
//        SqlParser.Config parserConfig = SqlParser.config().withLex(Lex.MYSQL).withUnquotedCasing(UNCHANGED).withCaseSensitive(false).withQuoting(DOUBLE_QUOTE).withConformance(SqlConformanceEnum.MYSQL_5);
//        FrameworkConfig config = Frameworks.newConfigBuilder().parserConfig(parserConfig).defaultSchema(rewriter.rootSchema).build();

////        RelOptMaterialization
//        RelBuilder relBuilder = RelBuilder.create(config);
//        RelNode mvQuery = relBuilder
//                .scan("customer")
//                .build();
//        SubstitutionVisitor substitutionVisitor = new SubstitutionVisitor(testRelNode, testRelNode);
//        List<RelNode> materializations = substitutionVisitor.go(mvQuery);
//        System.out.println(materializations.getFirst().explain());
//        RelOptMaterialization relOptMaterialization = RelOptMaterialization
//        hepPlanner.addMaterialization();


        RelNode rewrite_result = hepPlanner.findBestExp();
        System.out.println(testRelNode.explain());
        System.out.println(rewrite_result.explain());
        if (rewrite_result.deepEquals(testRelNode)) {
            System.out.println("No changed!");
            return;
        }
        String rewrite_sql = converter.visitRoot(rewrite_result).asStatement().toSqlString(PostgresqlSqlDialect.DEFAULT).getSql();
        System.out.println("TEST SQL =================");
        System.out.println(testSql);

        System.out.println("REWRITE SQL =================");
        System.out.println(rewrite_sql);


    }
}
