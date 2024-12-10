import React, { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import './SearchBarHome.css'; // Add necessary styles here
import searchIcon from '../../../assets/pictures/search.png';

const SearchBarHome = ({ players }) => {
  const [searchTerm, setSearchTerm] = useState('');
  const [filteredPlayers, setFilteredPlayers] = useState([]);
  const [showSuggestions, setShowSuggestions] = useState(false);
  const navigate = useNavigate();

  const handleInputChange = (e) => {
    const term = e.target.value;
    setSearchTerm(term);
    if (term.length > 0) {
      const suggestions = players.filter(player =>
        player.toLowerCase().includes(term.toLowerCase())
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
    navigate(`/player/${player.replace(/\s+/g, '-').toLowerCase()}`);
  };

  return (
    <div className="search-box-home">
      <input
        type="text"
        className="search-input-home"
        placeholder="Search for a player..."
        value={searchTerm}
        onChange={handleInputChange}
      />
      <img src={searchIcon} className='search-icon-home' />
      {showSuggestions && (
        <ul className="suggestions-list-home">
          {filteredPlayers.map((player, index) => (
            <li key={index} onClick={() => handlePlayerClick(player)}>
              {player}
            </li>
          ))}
        </ul>
      )}
    </div>
  );
};

export default SearchBarHome;