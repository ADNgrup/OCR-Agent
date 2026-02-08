from typing import Dict, Any, List, Optional
from modules.ocr.interface import OCRResult
from modules.ocr.layout import LayoutProcessor
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from datetime import datetime
import logging
import os
import json
import time

logger = logging.getLogger(__name__)


def log_ocr_run(mode: str, input_path: str, result: OCRResult, execution_time: float):
    os.makedirs('logs/ocr', exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = f'logs/ocr/{mode}_{timestamp}.json'
    
    log_data = {
        'timestamp': datetime.now().isoformat(),
        'mode': mode,
        'input_path': input_path,
        'execution_time_seconds': round(execution_time, 2),
        'result': {
            'text': result.text,
            'confidence': result.confidence,
            'metadata': result.metadata
        }
    }
    
    with open(log_file, 'w', encoding='utf-8') as f:
        json.dump(log_data, f, ensure_ascii=False, indent=2)
    
    logger.info(f"OCR run logged to: {log_file}")


class OCRProcessor:
    
    def __init__(self, ocr_engines: Dict[str, Any], llm_provider: Optional[Any] = None):
        self.ocr_engines = ocr_engines
        self.llm_provider = llm_provider
    
    def process_fast(self, input_path: str) -> OCRResult:
        start_time = time.time()
        
        pipeline_steps = []
        
        glm_ocr = self.ocr_engines.get('glm-ocr')
        
        if not glm_ocr:
            raise ValueError("GLM-OCR engine required for fast mode")
        
        if not self.llm_provider:
            raise ValueError("LLM provider required for fast mode")
        
        logger.info("Fast mode: Parallel execution - Step 1 (Visual) + Step 2 (OCR)")
        
        with ThreadPoolExecutor(max_workers=2) as executor:
            future_visual = executor.submit(
                self.llm_provider.detect_visual_elements,
                input_path
            )
            
            future_ocr = executor.submit(
                glm_ocr.process,
                input_path,
                task="text"
            )
            
            visual_response = future_visual.result()
            glm_result = future_ocr.result()
        
        pipeline_steps.extend(['qwen3-vl-visual', 'glm-ocr'])
        
        logger.info("Fast mode: Step 3 - Integration (text-only)")
        try:
            final_response = self.llm_provider.integrate_results_text_only(
                visual_response.text,
                glm_result.text
            )
            pipeline_steps.append('qwen3-vl-integration-text')
            ocr_text = final_response.text
            confidence = glm_result.confidence
        except Exception as e:
            logger.warning(f"Integration failed: {str(e)}, using GLM text only")
            ocr_text = glm_result.text
            confidence = glm_result.confidence
        
        result = OCRResult(
            text=ocr_text,
            boxes=[],
            confidence=confidence,
            metadata={
                'mode': 'fast-parallel',
                'pipeline': pipeline_steps,
                'engine': 'qwen3vl+glm-ocr',
                'visual_elements': visual_response.text
            }
        )
        
        execution_time = time.time() - start_time
        log_ocr_run('fast', input_path, result, execution_time)
        
        return result
    
    def process_thinking(self, input_path: str) -> OCRResult:
        start_time = time.time()
        
        pipeline_steps = []
        
        marker = self.ocr_engines.get('marker')
        glm_ocr = self.ocr_engines.get('glm-ocr')
        
        if not glm_ocr:
            raise ValueError("GLM-OCR engine required for thinking mode")
        
        blocks = []
        block_results = []
        
        file_ext = Path(input_path).suffix.lower()
        is_pdf = file_ext == '.pdf'
        
        if marker and is_pdf:
            logger.info("Thinking mode: Step 1 - Marker layout detection")
            try:
                layout_proc = LayoutProcessor(marker)
                blocks = layout_proc.extract_layout_blocks(input_path)
                pipeline_steps.append('marker-layout')
                
                if blocks:
                    logger.info(f"Thinking mode: Step 2 - GLM-OCR processing {len(blocks)} blocks")
                    
                    for block in blocks:
                        try:
                            tmp_path = layout_proc.save_cropped_block(input_path, block['bbox'])
                            try:
                                block_ocr = glm_ocr.process(tmp_path, task="text")
                                block_results.append({
                                    'block_id': block['id'],
                                    'bbox': block['bbox'],
                                    'type': block.get('type', 'text'),
                                    'text': block_ocr.text,
                                    'confidence': block_ocr.confidence
                                })
                            finally:
                                if os.path.exists(tmp_path):
                                    os.unlink(tmp_path)
                        except Exception as e:
                            logger.warning(f"Block {block['id']} OCR failed: {str(e)}")
                    
                    pipeline_steps.append('glm-ocr-blocks')
            except Exception as e:
                logger.warning(f"Marker failed: {str(e)}, falling back to full image")
        elif not is_pdf:
            logger.info("Thinking mode: Skipping Marker (image file, not PDF)")
        
        if not block_results:
            logger.info("Thinking mode: Fallback - GLM-OCR full image")
            glm_result = glm_ocr.process(input_path, task="text")
            pipeline_steps.append('glm-ocr-fallback')
            combined_text = glm_result.text
            combined_confidence = glm_result.confidence
        else:
            combined_text = '\n\n'.join([b['text'] for b in block_results if b['text']])
            confidences = [b['confidence'] for b in block_results if b['confidence'] > 0]
            combined_confidence = sum(confidences) / len(confidences) if confidences else 0.0
        
        if self.llm_provider and hasattr(self.llm_provider, 'structure_blocks'):
            logger.info("Thinking mode: Step 3 - Qwen3VL structure analysis")
            try:
                blocks_for_analysis = block_results if block_results else [{'id': 0, 'text': combined_text, 'type': 'full'}]
                
                qwen_response = self.llm_provider.structure_blocks(input_path, blocks_for_analysis)
                pipeline_steps.append('qwen3-vl-structure')
                combined_text = qwen_response.text
                    
            except Exception as e:
                logger.warning(f"Qwen3VL structuring failed: {str(e)}")
        
        result = OCRResult(
            text=combined_text,
            boxes=[],
            confidence=combined_confidence,
            metadata={
                'mode': 'thinking',
                'pipeline': pipeline_steps,
                'blocks_count': len(blocks),
                'blocks': block_results,
                'engine': 'layout-aware'
            }
        )
        
        execution_time = time.time() - start_time
        log_ocr_run('thinking', input_path, result, execution_time)
        
        return result
