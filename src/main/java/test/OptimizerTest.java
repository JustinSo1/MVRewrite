package test;

import com.google.common.collect.ImmutableList;
import main.trait.Optimizer;
import main.trait.SimpleSchema;
import main.trait.SimpleTable;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.materialize.MaterializedViewRule;
import org.apache.calcite.rel.rules.materialize.MaterializedViewRules;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.*;
import org.junit.jupiter.api.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;
import java.util.List;

public class OptimizerTest {
    @Test
    public void test_tpch_q6() throws Exception {
        SimpleTable lineitem = SimpleTable.newBuilder("lineitem")
                .addField("l_quantity", SqlTypeName.DECIMAL)
                .addField("l_extendedprice", SqlTypeName.DECIMAL)
                .addField("l_discount", SqlTypeName.DECIMAL)
                .addField("l_shipdate", SqlTypeName.DATE)
                .addField("l_orderkey", SqlTypeName.INTEGER)
                .addField("l_tax", SqlTypeName.DECIMAL)
                .withRowCount(60_000L)
                .build();

        SimpleSchema schema = SimpleSchema.newBuilder("tpch").addTable(lineitem).build();

        Optimizer optimizer = Optimizer.create(schema);

        String sql =
                "select\n" +
                        "    sum(l.l_extendedprice * l.l_discount) as revenue\n" +
                        "from\n" +
                        "    lineitem l\n" +
                        "where\n" +
                        "    l.l_shipdate >= ?\n" +
                        "    and l.l_shipdate < ?\n" +
                        "    and l.l_discount between (? - 0.01) AND (? + 0.01)\n" +
                        "    and l.l_quantity < ?";
        SqlNode sqlTree = optimizer.parse(sql);
        SqlNode validatedSqlTree = optimizer.validate(sqlTree);
        RelNode relTree = optimizer.convert(validatedSqlTree);

        print("AFTER CONVERSION", relTree);

        RuleSet rules = RuleSets.ofList(
                CoreRules.FILTER_TO_CALC,
                CoreRules.PROJECT_TO_CALC,
                CoreRules.FILTER_CALC_MERGE,
                CoreRules.PROJECT_CALC_MERGE,
                EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE,
                EnumerableRules.ENUMERABLE_PROJECT_RULE,
                EnumerableRules.ENUMERABLE_FILTER_RULE,
                EnumerableRules.ENUMERABLE_CALC_RULE,
                EnumerableRules.ENUMERABLE_AGGREGATE_RULE
        );

//        optimizer.planner.setRoot(relTree);


        RelNode optimizerRelTree = optimizer.optimize(
                relTree,
                relTree.getTraitSet().plus(EnumerableConvention.INSTANCE),
                rules,
                Collections.emptyList()
        );
        print("AFTER OPTIMIZATION", optimizerRelTree);
    }

    @Test
    public void test_tpch_q6_materialization() throws Exception {
        SimpleTable lineitem = SimpleTable.newBuilder("lineitem")
                .addField("l_quantity", SqlTypeName.DECIMAL)
                .addField("l_extendedprice", SqlTypeName.DECIMAL)
                .addField("l_discount", SqlTypeName.DECIMAL)
                .addField("l_shipdate", SqlTypeName.DATE)
                .addField("l_orderkey", SqlTypeName.INTEGER)
                .addField("l_tax", SqlTypeName.DECIMAL)
                .withRowCount(60_000L)
                .build();

        SimpleTable mv = SimpleTable.newBuilder("mv0")
                .addField("l_discount", SqlTypeName.DECIMAL)
                .addField("l_tax", SqlTypeName.DECIMAL)
                .withRowCount(3_000L)
                .build();

        SimpleSchema schema = SimpleSchema.newBuilder("tpch").addTable(lineitem).addTable(mv).build();

        Optimizer optimizer = Optimizer.create(schema);

        String sql = "select l_discount, sum(l_tax)\n" +
                "from lineitem\n" +
                "where l_discount > 100 group by l_discount";
        SqlNode sqlTree = optimizer.parse(sql);
        SqlNode validatedSqlTree = optimizer.validate(sqlTree);
        RelNode queryRel = optimizer.convert(validatedSqlTree);

        String mvSQL = "select * from mv0";
        SqlNode mvsqlTree = optimizer.parse(mvSQL);
        SqlNode mvvalidatedSqlTree = optimizer.validate(mvsqlTree);
        RelNode mvtableRel = optimizer.convert(mvvalidatedSqlTree);

        print("AFTER CONVERSION", queryRel);
//        System.out.println(queryRel.explain());
        /*
        LogicalAggregate(group=[{0}], EXPR$1=[SUM($1)])
            LogicalProject(l_discount=[$2], l_tax=[$5])
                LogicalFilter(condition=[>($2, 100)])
                    LogicalTableScan(table=[[tpch, lineitem]])
         */
        RelOptMaterialization mat1 =
                new RelOptMaterialization(mvtableRel, queryRel, null,
                        ImmutableList.of("default", "mv"));
        optimizer.planner.addMaterialization(mat1);
//        optimizer.planner.registerAbstractRelationalRules();
        RuleSet rules = RuleSets.ofList(
                CoreRules.FILTER_TO_CALC,
                CoreRules.PROJECT_TO_CALC,
                CoreRules.FILTER_CALC_MERGE,
                CoreRules.PROJECT_CALC_MERGE,
//                MaterializedViewRules.PROJECT_JOIN,
//                MaterializedViewRules.PROJECT_AGGREGATE,
//                MaterializedViewRules.JOIN,
//                MaterializedViewRules.AGGREGATE,
//                MaterializedViewRules.FILTER,
//                MaterializedViewRules.FILTER_SCAN,
//                MaterializedViewRules.PROJECT_FILTER,
                EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE,
                EnumerableRules.ENUMERABLE_PROJECT_RULE,
                EnumerableRules.ENUMERABLE_FILTER_RULE,
                EnumerableRules.ENUMERABLE_CALC_RULE,
                EnumerableRules.ENUMERABLE_AGGREGATE_RULE
        );

        RelNode optimizerRelTree = optimizer.optimize(queryRel,
                queryRel.getTraitSet().plus(EnumerableConvention.INSTANCE),
                rules, List.of(mat1));
//        Program program = Programs.of(RuleSets.ofList(rules));
//
//        RelNode optimizerRelTree = program.run(
//                optimizer.planner,
//                queryRel,
//                queryRel.getTraitSet().plus(EnumerableConvention.INSTANCE),
//                List.of(mat1),
//                Collections.emptyList()
//        );
//        System.out.println(optimizerRelTree.explain());
        /*
        EnumerableProject(l_discount=[$0], EXPR$1=[$1])
            EnumerableTableScan(table=[[tpch, mv0]])
         */
        print("AFTER OPTIMIZATION", optimizerRelTree);
    }

    @Test
    public void test_tpch_q6_hepPlanner() throws Exception {
        String sql = "SELECT * FROM line_item";
        SqlParser.Config parserConfig = SqlParser.config()
                .withLex(Lex.MYSQL);
//                .withQuotedCasing(SqlParser.Config.DEFAULT.quotedCasing())
//                .withCaseSensitive(SqlParser.Config.DEFAULT.caseSensitive())
//                .withConformance(SqlParser.Config.DEFAULT.conformance());
        SqlParser sqlParser = SqlParser.create(sql, parserConfig);
        CalciteSchema rootSchema = CalciteSchema.createRootSchema(false, false);

        // This planner does not do anything.
        // We use a series of planner stages later to perform the real optimizations.
        RelOptPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        planner.setExecutor(RexUtil.EXECUTOR);
//        RelBuilder relBuilder = RelBuilder.create(config);
//        RelNode relNode = relBuilder
//                .scan("emps")
//                .scan("depts")
//                .join(JoinRelType.INNER, "deptno")
//                .filter(relBuilder.equals(relBuilder.field("empid"), relBuilder.literal(100)))
//                .build();
//        HepProgram hepProgram = HepProgram.builder()
//                .addRuleInstance(CoreRules.FILTER_INTO_JOIN)
//                .build();
//        HepPlanner hepPlanner = new HepPlanner(hepProgram);
    }


    private void print(String header, RelNode relTree) {
        StringWriter sw = new StringWriter();

        sw.append(header).append(":").append("\n");

        RelWriterImpl relWriter = new RelWriterImpl(new PrintWriter(sw), SqlExplainLevel.ALL_ATTRIBUTES, true);

        relTree.explain(relWriter);

        System.out.println(sw);
    }
}
