import React from 'react';
import './GameLog.css';

export const GameLog = ({gameLog}) => {

    const headers = ["DATE", "OPP", "MIN", "PTS", "REB", "AST", "STL", "BLK"]

    return (
        <div className='GameLogContainer'>
            <div className='GameLogTitle'>
                <h1>Game Log</h1>
            </div>
            <table className='GameLogTable'>
                <thead>
                    <tr className='HeadersRow'>
                        {headers.map((header) => (
                            <th key={header}>{header}</th>
                        ))}
                    </tr>
                </thead>
            </table>
            <div className='ScrollableTable'>
                <table className='GameLogTable'>
                    <tbody>
                        {gameLog.slice().reverse().map((row, index) => (
                            <tr key={index} className='DataRow'>
                                <td>{row.date}</td>
                                <td>{row.matchup}</td>
                                <td>{row.min}</td>
                                <td>{row.pts}</td>
                                <td>{row.reb}</td>
                                <td>{row.ast}</td>
                                <td>{row.stl}</td>
                                <td>{row.blk}</td>
                            </tr>
                        ))}
                    </tbody>
                </table>
            </div>
        </div>
    );
};

export default GameLog;