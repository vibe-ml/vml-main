"""Live API checks. Run: python3 test_embeddings.py [http://127.0.0.1:30004]."""

import json
import math
import os
import sys
import unittest
from concurrent.futures import ThreadPoolExecutor
from urllib.error import HTTPError
from urllib.request import Request, urlopen

BASE_URL = sys.argv.pop(1).rstrip("/") if len(sys.argv) > 1 else "http://127.0.0.1:30004"
MODEL = "microsoft/harrier-oss-v1-0.6b"


def request(path, payload=None):
    headers = {"Content-Type": "application/json"}
    if os.environ.get("VLLM_API_KEY"):
        headers["Authorization"] = "Bearer " + os.environ["VLLM_API_KEY"]
    data = json.dumps(payload).encode() if payload is not None else None
    with urlopen(Request(BASE_URL + path, data=data, headers=headers), timeout=60) as response:
        body = response.read()
        return json.loads(body) if body else None


class EmbeddingsTest(unittest.TestCase):
    def embed(self, texts):
        result = request("/v1/embeddings", {
            "model": MODEL, "input": texts, "encoding_format": "float",
        })
        count = 1 if isinstance(texts, str) else len(texts)
        self.assertEqual(len(result["data"]), count)
        self.assertEqual([item["index"] for item in result["data"]], list(range(count)))
        self.assertGreater(result["usage"]["prompt_tokens"], 0)
        vectors = [item["embedding"] for item in result["data"]]
        for vector in vectors:
            self.assertEqual(len(vector), 1024)
            self.assertTrue(all(math.isfinite(value) for value in vector))
            self.assertAlmostEqual(math.sqrt(sum(value * value for value in vector)), 1, places=4)
        return vectors

    def test_health_and_model(self):
        request("/health")
        self.assertIn(MODEL, [item["id"] for item in request("/v1/models")["data"]])

    def test_single_and_batch_consistency(self):
        text = "Paris is the capital of France."
        single = self.embed(text)[0]
        batch = self.embed([text, "A much shorter sentence."])[0]
        self.assertGreater(sum(a * b for a, b in zip(single, batch)), 0.9999)

    def test_retrieval(self):
        query, relevant, unrelated = self.embed([
            "Instruct: Given a web search query, retrieve relevant passages that answer the query\nQuery: What is the capital of France?",
            "Paris is the capital and largest city of France.",
            "Banana bread is made with ripe bananas, flour, and eggs.",
        ])
        similarity = lambda vector: sum(a * b for a, b in zip(query, vector))
        self.assertGreater(similarity(relevant), similarity(unrelated) + 0.1)

    def test_unicode(self):
        self.embed(["Madrid es la capital de España.", "東京は日本の首都です。"])

    def test_concurrent_requests(self):
        with ThreadPoolExecutor(max_workers=4) as executor:
            list(executor.map(self.embed, [f"Concurrent request number {i}." for i in range(4)]))

    def test_context_limit(self):
        with self.assertRaises(HTTPError) as error:
            request("/v1/embeddings", {"model": MODEL, "input": "hello " * 5000})
        self.assertEqual(error.exception.code, 400)

    def test_invalid_model(self):
        with self.assertRaises(HTTPError) as error:
            request("/v1/embeddings", {"model": "missing-model", "input": "Hello"})
        self.assertEqual(error.exception.code, 404)


if __name__ == "__main__":
    unittest.main(verbosity=2)
