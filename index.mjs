import { MultiFileLoader } from "langchain/document_loaders/fs/multi_file";
import {
  JSONLoader,
} from "langchain/document_loaders/fs/json";
// import { TextLoader } from "langchain/document_loaders/fs/text";
// import { CSVLoader } from "langchain/document_loaders/fs/csv";
import { MemoryVectorStore } from 'langchain/vectorstores/memory';
import { OpenAIEmbeddings } from "@langchain/openai";
import { FaissStore } from "@langchain/community/vectorstores/faiss";
import { HNSWLib } from "@langchain/community/vectorstores/hnswlib";

// Load environment variables if needed
import dotenv from 'dotenv';
dotenv.config(); // Make sure OPENAI_API_KEY is set

import FileHound from "filehound";

async function getFiles(folderPath) {
  const files = await FileHound.create()
    .paths(folderPath)
    .ext('json')
    .find();
  return files.splice(0, 1);
}

async function loadFiles(files) {
  const embeddings = new OpenAIEmbeddings(); // Uses OpenAI API under the hood

  const vectorStore = await HNSWLib.fromDocuments([], embeddings);

  for (const file of files) {
    const loader = new JSONLoader(
      file,
      [
        "/type",
        "/contig",
        "/start",
        "/stop",
        "/strand",
        "/frame",
        "/gene",
        "/product",
        "/db_xrefs",
        "/aa_hexdigest",
        "/start_type",
        "/rbs_motif",
        "/ups",
        "/ips",
        "/psc",
        "/pscc",
        "/genes",
        "/id",
        "/locus",
      ]
    );
    const docs = await loader.load();
    vectorStore.addDocuments(docs);
  }

  return vectorStore;
}

async function main() {
  const files = await getFiles("/Users/ka10/Downloads/atb.bakta.incr_release.202408.batch.1");
  const vectorStore = await loadFiles(files)
  await vectorStore.save("./store");
}

await main();




