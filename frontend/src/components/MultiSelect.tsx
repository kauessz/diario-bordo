// frontend/src/components/MultiSelect.tsx
import React, { useState, useRef, useEffect } from 'react';

interface MultiSelectProps {
  options: string[];
  selected: string[];
  onChange: (selected: string[]) => void;
  label: string;
  placeholder?: string;
}

export default function MultiSelect({
  options,
  selected,
  onChange,
  label,
  placeholder = "Selecione..."
}: MultiSelectProps) {
  const [isOpen, setIsOpen] = useState(false);
  const [searchTerm, setSearchTerm] = useState("");
  const dropdownRef = useRef<HTMLDivElement>(null);

  // Fechar dropdown ao clicar fora
  useEffect(() => {
    function handleClickOutside(event: MouseEvent) {
      if (dropdownRef.current && !dropdownRef.current.contains(event.target as Node)) {
        setIsOpen(false);
      }
    }
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  const filteredOptions = options.filter(option =>
    option.toLowerCase().includes(searchTerm.toLowerCase())
  );

  const toggleOption = (option: string) => {
    if (selected.includes(option)) {
      onChange(selected.filter(s => s !== option));
    } else {
      onChange([...selected, option]);
    }
  };

  const selectAll = () => onChange(filteredOptions);
  const clearAll = () => onChange([]);

  return (
    <div className="multi-select-container" ref={dropdownRef}>
      <label className="lbl">{label}</label>
      
      <div 
        className="multi-select-trigger"
        onClick={() => setIsOpen(!isOpen)}
      >
        <span className="multi-select-value">
          {selected.length === 0 && placeholder}
          {selected.length === 1 && selected[0]}
          {selected.length > 1 && `${selected.length} selecionados`}
        </span>
        <span className="multi-select-arrow">{isOpen ? '▲' : '▼'}</span>
      </div>

      {isOpen && (
        <div className="multi-select-dropdown">
          <div className="multi-select-search">
            <input
              type="text"
              placeholder="Buscar..."
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              onClick={(e) => e.stopPropagation()}
            />
          </div>

          <div className="multi-select-actions">
            <button 
              className="btn-mini"
              onClick={(e) => { e.stopPropagation(); selectAll(); }}
            >
              Selecionar todos
            </button>
            <button 
              className="btn-mini"
              onClick={(e) => { e.stopPropagation(); clearAll(); }}
            >
              Limpar
            </button>
          </div>

          <div className="multi-select-options">
            {filteredOptions.length === 0 ? (
              <div className="multi-select-empty">Nenhum resultado encontrado</div>
            ) : (
              filteredOptions.map((option) => (
                <label 
                  key={option} 
                  className="multi-select-option"
                  onClick={(e) => e.stopPropagation()}
                >
                  <input
                    type="checkbox"
                    checked={selected.includes(option)}
                    onChange={() => toggleOption(option)}
                  />
                  <span className="multi-select-option-text">{option}</span>
                </label>
              ))
            )}
          </div>

          <div className="multi-select-footer">
            {selected.length} de {options.length} selecionado(s)
          </div>
        </div>
      )}
    </div>
  );
}