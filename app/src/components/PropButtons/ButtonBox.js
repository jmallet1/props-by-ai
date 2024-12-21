import React, { useState } from 'react';
import './ButtonBox.css';

const ButtonBox = ({ availableProps, updateData}) => {

  const [selectedProp, setSelectedProp] = useState(0);

  const handlePropClick = (index, type) => {
    setSelectedProp(index);
    updateData(type);
  };

  return (
    <div className="propButtonBox">
      {availableProps.map((type, index) => (
        <button
          key={type} // Use the prop type as a unique key
          onClick={() => handlePropClick(index, type)}
          className={selectedProp === index ? 'active' : ''}
        >
          {type === 'pts' && 'Points'}
          {type === 'reb' && 'Rebounds'}
          {type === 'ast' && 'Assists'}
          {type === 'blk' && 'Blocks'}
          {type === 'stl' && 'Steals'}
          {type === 'to' && 'Turnovers'}
        </button>
      ))}
    </div>
  );
};

export default ButtonBox;