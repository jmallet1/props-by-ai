import React, { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import './SearchBar.css'; // Add necessary styles here
import searchIcon from '../../assets/pictures/search.png';

const SearchBar = ({ playerList }) => {
  const [searchTerm, setSearchTerm] = useState('');
  const [filteredPlayers, setFilteredPlayers] = useState([]);
  const [showSuggestions, setShowSuggestions] = useState(false);
  const navigate = useNavigate();

  const handleInputChange = (e) => {
    const term = e.target.value;
    setSearchTerm(term);
    if (term.length > 0) {
      const suggestions = playerList.filter(player =>
        player.name.toLowerCase().includes(term.toLowerCase())
      );
      setFilteredPlayers(suggestions);
      setShowSuggestions(true);
    } else {
      setShowSuggestions(false);
    }
  };

  const handlePlayerClick = (player) => {
    setSearchTerm('');
    setShowSuggestions(false);
    navigate(`/player/${player.id}`);
  };

  return (
    <div className="search-box">
      <input
        type="text"
        className="search-input"
        placeholder="Search for a player..."
        value={searchTerm}
        onChange={handleInputChange}
      />
      <img src={searchIcon} className='search-icon' alt='search'/>
      {showSuggestions && (
        <ul className="suggestions-list">
          {filteredPlayers.map((player, index) => (
            <li key={index} onClick={() => handlePlayerClick(player)}>
              {player.name}
            </li>
          ))}
        </ul>
      )}
    </div>
  );
};

export default SearchBar;