import logging
import sys
import json
import os

from sd_parsers import ParserManager
from sd_parsers.data import Sampler

parser_manager = ParserManager()

def show_sampler(i: int, sampler: Sampler):
    print(f"\n{'#'*80}\nSampler #{i+1}: {sampler.name}\n{'#'*80}")

    if sampler.model:
        print(f"\nModel: {sampler.model}")

    if sampler.prompts:
        print("\nPrompts:")
        for prompt in sampler.prompts:
            print(f"Prompt: {prompt}\nMetadata: {prompt.metadata}")

    if sampler.negative_prompts:
        print("\nNegative Prompts:")
        for prompt in sampler.negative_prompts:
            print(f"Prompt: {prompt}\nMetadata: {prompt.metadata}")

    print(f"\nSampler Parameters: {sampler.parameters}")

def serialize_obj(obj):
    if isinstance(obj, (str, int, float, bool)) or obj is None:
        return obj
    elif isinstance(obj, list):
        return [serialize_obj(item) for item in obj]
    elif isinstance(obj, dict):
        return {key: serialize_obj(value) for key, value in obj.items()}
    else:
        # For custom objects, convert __dict__ recursively
        if hasattr(obj, "__dict__"):
            return {key: serialize_obj(value) for key, value in obj.__dict__.items() if not key.startswith("_")}
        else:
            return str(obj)

def main():
    import_path = input("Enter the path to an image file: ").strip()
    if not os.path.isfile(import_path):
        print(f"File '{import_path}' does not exist.")
        return

    try:
        prompt_info = parser_manager.parse(import_path)
        if prompt_info:
            for i, sampler in enumerate(prompt_info.samplers):
                show_sampler(i, sampler)

            print(f"\nRemaining Metadata: {prompt_info.metadata}")

            # Serialize entire prompt_info object recursively
            metadata_obj = serialize_obj(prompt_info)
            print("DEBUG: serialized metadata_obj =", metadata_obj)

            if metadata_obj:
                json_path = os.path.splitext(import_path)[0] + ".json"
                with open(json_path, "w", encoding="utf-8") as f:
                    json.dump(metadata_obj, f, indent=2, ensure_ascii=False)
                print(f"Full JSON metadata saved to: {json_path}")

                # Save JSON keys to a .txt file
                keys = list(metadata_obj.keys()) if isinstance(metadata_obj, dict) else []
                txt_path = os.path.splitext(import_path)[0] + "_keys.txt"
                with open(txt_path, "w", encoding="utf-8") as f:
                    for key in keys:
                        f.write(key + "\\n")
                print(f"JSON keys saved to: {txt_path}")
        else:
            print("No metadata found in the image.")
    except Exception:
        logging.exception("Error reading file: %s", import_path)

if __name__ == "__main__":
    main()
