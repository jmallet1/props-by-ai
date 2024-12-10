import React, { useState } from 'react';
import './ButtonBox.css';

const ButtonBox = ({ updateData }) => {
  const [selectedProp, setSelectedProp] = useState(0);

  const handlePropClick = (index, type) => {
    setSelectedProp(index);
    updateData(type);
  };

  return (
    <>
    <div className="propButtonBox">
      <button onClick={() => handlePropClick(0, 'pts')} className={selectedProp === 0 ? 'active' : ''}>Points</button>
      <button onClick={() => handlePropClick(1, 'reb')} className={selectedProp === 1 ? 'active' : ''}>Rebounds</button>
      <button onClick={() => handlePropClick(2, 'ast')} className={selectedProp === 2 ? 'active' : ''}>Assists</button>
      <button onClick={() => handlePropClick(3, 'blk')} className={selectedProp === 3 ? 'active' : ''}>Blocks</button>
      <button onClick={() => handlePropClick(4, 'stl')} className={selectedProp === 4 ? 'active' : ''}>Steals</button>
      <button onClick={() => handlePropClick(5, 'to')} className={selectedProp === 5 ? 'active' : ''}>Turnovers</button>
    </div>
    </>
  );
};

export default ButtonBox;