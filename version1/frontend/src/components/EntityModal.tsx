/* eslint-disable @typescript-eslint/no-explicit-any */
/* eslint-disable @typescript-eslint/no-unused-vars */
import React, { useState, useEffect } from "react";

interface EntityModalProps {
  show: boolean;
  onClose: () => void;
  entityForm: any;
  setEntityForm: (obj: any) => void;
  editingEntityId: string | null;
  onSave: () => void;
}

export default function EntityModal({ show, onClose, entityForm, setEntityForm, editingEntityId, onSave }: EntityModalProps) {
  if (!show) return null;

  return (
    <div style={{
      position: "fixed", top: 0, left: 0, right: 0, bottom: 0,
      background: "rgba(0,0,0,0.5)", zIndex: 9999, display: "flex",
      alignItems: "center", justifyContent: "center"
    }}>
      <div style={{
        background: "#fff", padding: "20px", borderRadius: "8px", width: "600px",
        maxHeight: "90vh", overflowY: "auto", display: "flex", flexDirection: "column", gap: "12px",
        color: "#111"
      }}>
        <h3>{editingEntityId ? "Edit Entity Schema" : "Add Entity Schema"}</h3>

        <label style={{ display: "flex", flexDirection: "column", gap: 4 }}>
          <strong>Main Entity Name (exactly as shown on screen)</strong>
          <input type="text" placeholder="Example: Boiler Temp" value={entityForm.main_entity_name} onChange={e => setEntityForm({...entityForm, main_entity_name: e.target.value})} />
        </label>

        <label style={{ display: "flex", flexDirection: "column", gap: 4 }}>
          <strong>Type</strong>
          <select value={entityForm.type} onChange={e => setEntityForm({...entityForm, type: e.target.value})}>
            <option value="HMI Object">HMI Object</option>
            <option value="table">Table</option>
            <option value="log/alert">Log/Alert</option>
            <option value="switch">Switch</option>
            <option value="sensor">Sensor</option>
            <option value="display">Display</option>
          </select>
        </label>

        <label style={{ display: "flex", flexDirection: "column", gap: 4 }}>
          <strong>Region</strong>
          <select value={entityForm.region} onChange={e => setEntityForm({...entityForm, region: e.target.value})}>
            <option value="center">center</option>
            <option value="top_left">top_left</option>
            <option value="top_center">top_center</option>
            <option value="top_right">top_right</option>
            <option value="center_diagram">center_diagram</option>
            <option value="bottom_pumps">bottom_pumps</option>
            <option value="overlay_popup">overlay_popup</option>
          </select>
        </label>

        {entityForm.type?.toLowerCase() === "table" ? (
          <div>
            <strong style={{ display: "block", marginBottom: 8 }}>Sub-entities (Cells)</strong>
            {entityForm.subentities && entityForm.subentities.map((sub: any, idx: number) => (
              <div key={sub._id || idx} style={{ display: "flex", gap: 8, marginBottom: 8, alignItems: "center" }}>
                <input type="text" placeholder="Col name" value={sub.col} onChange={e => {
                  const newSubs = [...entityForm.subentities];
                  newSubs[idx] = { ...newSubs[idx], col: e.target.value };
                  setEntityForm({...entityForm, subentities: newSubs});
                }} style={{ flex: 1, width: "80px" }} />
                <input type="text" placeholder="Row name" value={sub.row} onChange={e => {
                  const newSubs = [...entityForm.subentities];
                  newSubs[idx] = { ...newSubs[idx], row: e.target.value };
                  setEntityForm({...entityForm, subentities: newSubs});
                }} style={{ flex: 1, width: "80px" }} />
                <input type="text" placeholder="Unit" value={sub.unit} onChange={e => {
                  const newSubs = [...entityForm.subentities];
                  newSubs[idx] = { ...newSubs[idx], unit: e.target.value };
                  setEntityForm({...entityForm, subentities: newSubs});
                }} style={{ width: "60px" }} />
                <select value={sub.value_type} onChange={e => {
                  const newSubs = [...entityForm.subentities];
                  newSubs[idx] = { ...newSubs[idx], value_type: e.target.value };
                  setEntityForm({...entityForm, subentities: newSubs});
                }}>
                  <option value="text">text</option>
                  <option value="number">number</option>
                  <option value="color">color</option>
                  <option value="bool">bool</option>
                </select>
                <button className="btn-sm btn-secondary" style={{ color: "red" }} onClick={() => {
                  const newSubs = entityForm.subentities.filter((_: any, i: number) => i !== idx);
                  setEntityForm({...entityForm, subentities: newSubs});
                }}>X</button>
              </div>
            ))}
            <button className="btn-sm btn-secondary" onClick={() => {
              setEntityForm({
                ...entityForm,
                subentities: [...(entityForm.subentities || []), { _id: Math.random().toString(36).substring(2, 9), col: "", row: "", unit: "", value_type: "text" }]
              });
            }}>+ Add Table Cell</button>
          </div>
        ) : entityForm.type?.toLowerCase() === "log/alert" || entityForm.type?.toLowerCase() === "log" ? (
          <div>
            <p className="muted" style={{ fontSize: "0.85rem" }}>No specific schema needed for Logs. They will be captured dynamically as lists.</p>
          </div>
        ) : (
          <div>
            <strong style={{ display: "block", marginBottom: 8 }}>Indicators</strong>
            {entityForm.indicators && entityForm.indicators.map((ind: any, idx: number) => (
              <div key={ind._id || idx} style={{ display: "flex", gap: 8, marginBottom: 8, alignItems: "center" }}>
                <input type="text" placeholder="Label" value={ind.label} onChange={e => {
                  const newInds = [...entityForm.indicators];
                  newInds[idx] = { ...newInds[idx], label: e.target.value };
                  setEntityForm({...entityForm, indicators: newInds});
                }} style={{ flex: 1, width: "100px" }} />
                <input type="text" placeholder="Metric" value={ind.metric} onChange={e => {
                  const newInds = [...entityForm.indicators];
                  newInds[idx] = { ...newInds[idx], metric: e.target.value };
                  setEntityForm({...entityForm, indicators: newInds});
                }} style={{ flex: 1, width: "100px" }} />
                <input type="text" placeholder="Unit" value={ind.unit} onChange={e => {
                  const newInds = [...entityForm.indicators];
                  newInds[idx] = { ...newInds[idx], unit: e.target.value };
                  setEntityForm({...entityForm, indicators: newInds});
                }} style={{ width: "60px" }} />
                <select value={ind.value_type} onChange={e => {
                  const newInds = [...entityForm.indicators];
                  newInds[idx] = { ...newInds[idx], value_type: e.target.value };
                  setEntityForm({...entityForm, indicators: newInds});
                }}>
                  <option value="text">text</option>
                  <option value="number">number</option>
                  <option value="color">color</option>
                  <option value="bool">bool</option>
                </select>
                <button className="btn-sm btn-secondary" style={{ color: "red" }} onClick={() => {
                  const newInds = entityForm.indicators.filter((_: any, i: number) => i !== idx);
                  setEntityForm({...entityForm, indicators: newInds});
                }}>X</button>
              </div>
            ))}
            <button className="btn-sm btn-secondary" onClick={() => {
              setEntityForm({
                ...entityForm,
                indicators: [...(entityForm.indicators || []), { _id: Math.random().toString(36).substring(2, 9), label: "", metric: "", unit: "", value_type: "text" }]
              });
            }}>+ Add Indicator</button>
          </div>
        )}

        <div style={{ display: "flex", gap: 8, marginTop: 16, justifyContent: "flex-end" }}>
          <button className="btn-secondary" onClick={onClose}>Cancel</button>
          <button className="btn-primary" onClick={onSave}>Save</button>
        </div>
      </div>
    </div>
  );
}
