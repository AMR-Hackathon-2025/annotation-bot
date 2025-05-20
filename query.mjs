import { SqlDatabase } from "langchain/sql_db";
import { DataSource } from "typeorm";
import { ChatOpenAI } from "@langchain/openai";
import { pull } from "langchain/hub";
import { ChatPromptTemplate, PromptTemplate } from "@langchain/core/prompts";
import { z } from "zod";
import { QuerySqlTool } from "langchain/tools/sql";

const datasource = new DataSource({
  type: "postgres",
  host: "localhost",
  port: 5432,
  username: "postgres",
  password: "postgres",
  database: "bakta_annotations",
});
const db = await SqlDatabase.fromDataSourceParams({
  appDataSource: datasource,
});


const llm = new ChatOpenAI({
  model: "gpt-4.1",
  // model: "gpt-4o",
  temperature: 0
});

const queryPromptTemplate = await pull(
  "langchain-ai/sql-query-system-prompt"
);

queryPromptTemplate.promptMessages[0].lc_kwargs.prompt.template = `
Given an input question, create a syntactically correct {dialect} query to run to help find the answer. Unless the user specifies in his question a specific number of examples they wish to obtain, always limit your query to at most {top_k} results. You can order the results by a relevant column to return the most interesting examples in the database.

When the input question refers to a protein, find all the COG IDs in bacteria relating to that protein and create a query against these COG IDs in cog_id column in psc table.

Never query for all the columns from a specific table, only ask for a the few relevant columns given the question.

Pay attention to use only the column names that you can see in the schema description. Be careful to not query for columns that do not exist. Also, pay attention to which column is in which table.

Only use the following tables:
{table_info}'
`;

// const queryPromptTemplate = new PromptTemplate({
//   inputVariables: ["foo"],
//   template: "Say {foo}",
// });

queryPromptTemplate.promptMessages.forEach((message) => {
  console.log(message.lc_kwargs.prompt.template);
});


const queryOutput = z.object({
  query: z.string().describe("Syntactically valid SQL query."),
});

const structuredLlm = llm.withStructuredOutput(queryOutput);

const writeQuery = async (state) => {
  const tableInfo = await db.getTableInfo();
  const promptValue = await queryPromptTemplate.invoke({
    dialect: db.appDataSourceOptions.type,
    top_k: 10,
    table_info: tableInfo,
    input: state.question,
  });
  const result = await structuredLlm.invoke(promptValue);
  return { query: result.query };
};

const executeQuery = async (state) => {
  const executeQueryTool = new QuerySqlTool(db);
  return { result: await executeQueryTool.invoke(state.query) };
};

const generateAnswer = async (state) => {
  const promptValue =
    "Given the following user question, corresponding SQL query, " +
    "and SQL result, answer the user question.\n\n" +
    `Question: ${state.question}\n` +
    `SQL Query: ${state.query}\n` +
    `SQL Result: ${state.result}\n`;
  const response = await llm.invoke(promptValue);
  return { answer: response.content };
};

async function main() {
  const initialState = { 
    query: "",
    result: "",
    answer: "",
    question: ""
  };
  const question = { question: process.argv.slice(2).join(" ")};
  console.error(question)

  const query = await writeQuery({
    ...initialState,
    question,
  });
  console.error(query)

  const result = await executeQuery({
    ...initialState,
    ...query,
  });
  console.error(result)

  const answer = await generateAnswer({
    ...initialState,
    ...query,
    ...result
  });
  console.error(answer)

  console.log(answer.answer)
}

await main();
