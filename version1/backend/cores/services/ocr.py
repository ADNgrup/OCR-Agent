from paddleocr import PaddleOCR
import threading, cv2, unicodedata
paddle_ocr_model = PaddleOCR(
    text_detection_model_name="PP-OCRv5_mobile_det",
    text_recognition_model_name="PP-OCRv5_mobile_rec",
    use_doc_orientation_classify=False,
    use_doc_unwarping=False,
    use_textline_orientation=False
)
paddle_lock = threading.Lock()

def generate_layout_text(image_path: str) -> str:
    if not paddle_ocr_model:
        return ""
    
    try:
        # We use cv2 imread directly from the saved image to avoid memory format issues
        image = cv2.imread(image_path)
        
        if paddle_lock:
            with paddle_lock:
                output = paddle_ocr_model.predict(input=image)
        else:
            output = paddle_ocr_model.predict(input=image)
            
        if not output or not output[0]:
            return ""
        
        res = output[0]
        
        # Save to temp JSON to avoid issues with numpy arrays or complex internal objects
        import tempfile, uuid, os, json, shutil
        tmp_dir = os.path.join(tempfile.gettempdir(), f"ocr_tmp_{uuid.uuid4().hex}")
        os.makedirs(tmp_dir, exist_ok=True)
        
        try:
            res.save_to_json(tmp_dir)
            json_files = [f for f in os.listdir(tmp_dir) if f.endswith('.json')]
            if not json_files:
                return ""
            
            with open(os.path.join(tmp_dir, json_files[0]), 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            texts = data.get("rec_texts", [])
            boxes = data.get("rec_boxes", [])
        except Exception as e:
            print(f"Failed to read OCR output via JSON: {e}")
            return ""
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)
        
        if not texts or not boxes:
            return ""
            
        max_x = max([box[2] for box in boxes])
        max_y = max([box[3] for box in boxes])
        
        items = []
        for txt, box in zip(texts, boxes):
            items.append({
                'text': txt,
                'xmin': box[0], 'ymin': box[1],
                'xmax': box[2], 'ymax': box[3],
                'yc': (box[1] + box[3]) / 2,
                'h': box[3] - box[1]
            })

        if items:
            avg_h = sum(item['h'] for item in items) / len(items)
            items.sort(key=lambda it: it['yc'])
            current_y = items[0]['ymin']
            current_yc = items[0]['yc']
            for item in items:
                if abs(item['yc'] - current_yc) < avg_h * 0.5:
                    item['ymin'] = current_y
                else:
                    current_y = item['ymin']
                    current_yc = item['yc']
            items.sort(key=lambda it: it['xmin'])
            current_x = items[0]['xmin']
            for item in items:
                if abs(item['xmin'] - current_x) < avg_h * 1.0:
                    item['xmin'] = current_x
                else:
                    current_x = item['xmin']

        def get_char_width(c):
            return 2 if unicodedata.east_asian_width(c) in ('F', 'W', 'A') else 1

        visual_width = 300 
        height_lines = int((max_y / max_x) * visual_width * 0.5) if max_x > 0 else 50
        lines_dict = {y: {} for y in range(height_lines + 1)}

        for item in items:
            xmin, ymin, txt = item['xmin'], item['ymin'], item['text']
            x = int((xmin / max_x) * visual_width)
            y = int((ymin / max_y) * height_lines)
            curr_x = x
            for char in txt:
                lines_dict[y][curr_x] = char
                curr_x += get_char_width(char)

        layout_lines = []
        for y in range(height_lines + 1):
            line_dict = lines_dict.get(y, {})
            if not line_dict:
                layout_lines.append("")
                continue
            line_str = ""
            current_x = 0
            for x_pos in sorted(line_dict.keys()):
                if x_pos > current_x:
                    line_str += " " * (x_pos - current_x)
                    current_x = x_pos
                char = line_dict[x_pos]
                line_str += char
                current_x += get_char_width(char)
            layout_lines.append(line_str.rstrip())
            
        return "\n".join(layout_lines)
        
    except Exception as e:
        print("OCR layout generation failed:", e)
        return ""
