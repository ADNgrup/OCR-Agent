# from transformers import AutoProcessor, AutoModelForImageTextToText
# import torch

# MODEL_PATH = "zai-org/GLM-OCR"
# messages = [
#     {
#         "role": "user",
#         "content": [
#             {
#                 "type": "image",
#                 "url": "./storage/snapshots/src69b412a9fde21897eb394e2a_default_20260317_013903.png"
#             },
#             {
#                 "type": "text",
#                 "text": "The image is a HMI screen or control panel. The content is constructed as tables, so please extract all information and return in latex format:"
#             }
#         ],
#     }
# ]
# processor = AutoProcessor.from_pretrained(MODEL_PATH)
# model = AutoModelForImageTextToText.from_pretrained(
#     pretrained_model_name_or_path=MODEL_PATH,
#     torch_dtype="auto",
#     device_map="auto",
# )
# inputs = processor.apply_chat_template(
#     messages,
#     tokenize=True,
#     add_generation_prompt=True,
#     return_dict=True,
#     return_tensors="pt"
# ).to(model.device)
# inputs.pop("token_type_ids", None)
# generated_ids = model.generate(**inputs, max_new_tokens=8192)
# output_text = processor.decode(generated_ids[0][inputs["input_ids"].shape[1]:], skip_special_tokens=False)
# print(output_text)


from paddleocr import PaddleOCRVL

# NVIDIA GPU
# pipeline = PaddleOCRVL()
# Kunlunxin XPU
# pipeline = PaddleOCRVL(device="xpu")
# Hygon DCU
# pipeline = PaddleOCRVL(device="dcu")
# MetaX GPU
# pipeline = PaddleOCRVL(device="metax_gpu")
# Apple Silicon
pipeline = PaddleOCRVL(device="cpu")

# pipeline = PaddleOCRVL(use_doc_orientation_classify=True) # Use use_doc_orientation_classify to enable/disable document orientation classification model
# pipeline = PaddleOCRVL(use_doc_unwarping=True) # Use use_doc_unwarping to enable/disable document unwarping module
# pipeline = PaddleOCRVL(use_layout_detection=False) # Use use_layout_detection to enable/disable layout detection module

output = pipeline.predict("./storage/snapshots/src69b412a9fde21897eb394e2a_default_20260317_013903.png")
for res in output:
    res.print() ## Print the structured prediction output
    res.save_to_json(save_path="output") ## Save the current image's structured result in JSON format
    res.save_to_markdown(save_path="output") ## Save the current image's result in Markdown format