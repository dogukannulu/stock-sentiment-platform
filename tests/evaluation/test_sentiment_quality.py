"""
GEval evaluation of Claude Haiku sentiment scoring quality.
Runs against the 50-example golden dataset and reports label accuracy
and score direction calibration.

Cost: ~$0.05 for 50 examples with claude-haiku-4-5-20251001.

Run with:
    pip install deepeval
    pytest tests/evaluation/test_sentiment_quality.py -v -s

Note: Requires ANTHROPIC_API_KEY in .env
"""
import pytest
import json
import asyncio
import os
from dotenv import load_dotenv

load_dotenv()

# Skip entire module if deepeval not installed
deepeval = pytest.importorskip("deepeval", reason="pip install deepeval to run evaluation tests")

from deepeval import evaluate
from deepeval.metrics import GEval
from deepeval.test_case import LLMTestCase, LLMTestCaseParams
from deepeval.models.llms.anthropic_model import AnthropicModel
from deepeval.evaluate.configs import AsyncConfig
from flink_jobs.sentiment_job import score_batch

# Use Claude as the GEval judge (avoids needing an OpenAI key)
_judge = AnthropicModel(
    model="claude-haiku-4-5-20251001",
    api_key=os.environ.get("ANTHROPIC_API_KEY"),
)


@pytest.fixture(scope="module")
def golden_dataset():
    dataset_path = os.path.join(
        os.path.dirname(__file__), "golden_dataset.json"
    )
    with open(dataset_path) as f:
        return json.load(f)


faithfulness_metric = GEval(
    name="Sentiment Label Faithfulness",
    criteria=(
        "The predicted sentiment label (positive/negative/neutral) correctly "
        "reflects the financial sentiment expressed in the input text. "
        "Consider: sarcasm, context, financial domain conventions (e.g. a stock "
        "'falling despite strong fundamentals' is negative for the stock)."
    ),
    evaluation_params=[
        LLMTestCaseParams.INPUT,
        LLMTestCaseParams.ACTUAL_OUTPUT,
        LLMTestCaseParams.EXPECTED_OUTPUT,
    ],
    threshold=0.75,
    model=_judge,
    async_mode=False,
)

score_calibration_metric = GEval(
    name="Score Direction Calibration",
    criteria=(
        "The sentiment score's sign correctly matches the expected direction. "
        "Positive sentiment should yield score > 0, negative < 0, neutral ≈ 0 "
        "(within -0.3 to +0.3 range)."
    ),
    evaluation_params=[
        LLMTestCaseParams.INPUT,
        LLMTestCaseParams.ACTUAL_OUTPUT,
        LLMTestCaseParams.EXPECTED_OUTPUT,
    ],
    threshold=0.80,
    model=_judge,
    async_mode=False,
)


def test_sentiment_faithfulness(golden_dataset):
    """Test that Claude Haiku's sentiment labels match ground truth labels.
    Uses first 10 examples to stay within free-tier rate limits (50 RPM).
    GEval makes 2 API calls per test case, so 10 cases = 20 calls.
    Run test_score_direction_accuracy for the full 50-example coverage.
    """
    test_cases = []
    sample = golden_dataset[:10]

    for i in range(0, len(sample), 10):
        batch = sample[i:i + 10]
        posts = [
            {
                "post_id": str(j),
                "symbol":  item["symbol"],
                "text":    item["text"],
                "source":  "evaluation",
            }
            for j, item in enumerate(batch, start=i)
        ]

        results = asyncio.run(score_batch(posts))

        for item, result in zip(batch, results):
            test_cases.append(LLMTestCase(
                input=item["text"],
                actual_output=(
                    f"{result['sentiment']} (score: {result['score']:.2f})"
                ),
                expected_output=item["expected_sentiment"],
            ))

    eval_result = evaluate(
        test_cases,
        [faithfulness_metric, score_calibration_metric],
        async_config=AsyncConfig(run_async=True, max_concurrent=3, throttle_value=2),
    )

    passed = sum(1 for tr in eval_result.test_results if tr.success)
    total = len(eval_result.test_results)
    accuracy = passed / total * 100
    print(f"\nSentiment accuracy: {accuracy:.1f}% ({passed}/{total})")

    assert accuracy >= 70, (
        f"Sentiment accuracy {accuracy:.1f}% is below 70% threshold. "
        "Check ANTHROPIC_API_KEY and model availability."
    )


def test_score_direction_accuracy(golden_dataset):
    """
    Lightweight test: verify score direction (sign) matches expected direction
    without calling GEval — runs cheaply against the full dataset.
    """
    batch_size = 10
    correct = 0
    total = len(golden_dataset)

    for i in range(0, total, batch_size):
        batch = golden_dataset[i:i + batch_size]
        posts = [
            {
                "post_id": str(j),
                "symbol":  item["symbol"],
                "text":    item["text"],
                "source":  "evaluation",
            }
            for j, item in enumerate(batch, start=i)
        ]
        results = asyncio.run(score_batch(posts))

        for item, result in zip(batch, results):
            expected_dir = item["expected_score_direction"]
            actual_score = result["score"]

            if expected_dir == 1 and actual_score > 0:
                correct += 1
            elif expected_dir == -1 and actual_score < 0:
                correct += 1
            elif expected_dir == 0 and -0.3 <= actual_score <= 0.3:
                correct += 1

    accuracy = correct / total * 100
    print(f"\nScore direction accuracy: {accuracy:.1f}% ({correct}/{total})")
    assert accuracy >= 70, f"Score direction accuracy {accuracy:.1f}% below 70%"
